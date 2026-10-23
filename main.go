package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/h264reader"
)

func main() {
	// 1. Configure Pion to use a single UDP port (8443) so Docker networking is clean
	settingEngine := webrtc.SettingEngine{}

	udpListener, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IP{0, 0, 0, 0},
		Port: 8443,
	})
	if err != nil {
		log.Fatalf("Failed to bind UDP port 8443: %v", err)
	}
	settingEngine.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udpListener))

	// Advertise your public server IP so browsers outside Docker can reach UDP 8443
	publicIP := os.Getenv("PUBLIC_IP")
	if publicIP == "" {
		publicIP = "125.63.25.226"
	}
	settingEngine.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeHost)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	// 2. Built-in Browser Test Page
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(indexHTML))
	})

	// 3. WebRTC Signaling + FFmpeg Pipeline
	http.HandleFunc("/offer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			return
		}

		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		peerConnection, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		videoTrack, err := webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264},
			"video",
			"ffmpeg-stream",
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rtpSender, err := peerConnection.AddTrack(videoTrack)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Read incoming RTCP packets (required by WebRTC spec for congestion/feedback)
		go func() {
			rtcpBuf := make([]byte, 1500)
			for {
				if _, _, rtcpErr := rtpSender.Read(rtcpBuf); rtcpErr != nil {
					return
				}
			}
		}()

		// Context to cleanly kill FFmpeg when the browser disconnects
		ctx, cancel := context.WithCancel(context.Background())
		peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
			log.Printf("ICE Connection State changed: %s", state.String())
			if state == webrtc.ICEConnectionStateConnected {
				go streamFFmpegToTrack(ctx, videoTrack)
			}
			if state == webrtc.ICEConnectionStateDisconnected ||
				state == webrtc.ICEConnectionStateFailed ||
				state == webrtc.ICEConnectionStateClosed {
				cancel()
				peerConnection.Close()
			}
		})

		if err = peerConnection.SetRemoteDescription(offer); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		answer, err := peerConnection.CreateAnswer(nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		gatherComplete := webrtc.GatheringCompletePromise(peerConnection)
		if err = peerConnection.SetLocalDescription(answer); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		<-gatherComplete

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(peerConnection.LocalDescription())
	})

	fmt.Println("Streaming server listening on :8080 (Signaling) and :8443/udp (WebRTC Media)")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func streamFFmpegToTrack(ctx context.Context, track *webrtc.TrackLocalStaticSample) {
	// Spawns FFmpeg generating a live 640x480@30fps test pattern in ultra-low-latency H.264
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-re",
		"-f", "lavfi", "-i", "testsrc=size=640x480:rate=30",
		"-c:v", "libx264",
		"-pix_fmt", "yuv420p",
		"-profile:v", "baseline",
		"-level:v", "3.1",
		"-preset", "ultrafast",
		"-tune", "zerolatency",
		"-g", "30",
		"-f", "h264",
		"pipe:1",
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("FFmpeg stdout pipe error: %v", err)
		return
	}

	if err := cmd.Start(); err != nil {
		log.Printf("FFmpeg start error: %v", err)
		return
	}
	log.Println("FFmpeg process started for peer")

	h264, err := h264reader.NewReader(stdout)
	if err != nil {
		log.Printf("H264 reader error: %v", err)
		return
	}

	ticker := time.NewTicker(time.Second / 30)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping FFmpeg process for disconnected peer")
			return
		default:
			nal, err := h264.NextNAL()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Printf("NAL read error: %v", err)
				return
			}

			// Only sleep on actual video frame slices (NAL Unit Types 1 and 5)
			// SPS/PPS headers (7 and 8) are sent immediately
			isFrame := nal.UnitType == h264reader.NalUnitTypeCodedSliceNonIdr ||
				nal.UnitType == h264reader.NalUnitTypeCodedSliceIdr

			var duration time.Duration
			if isFrame {
				<-ticker.C
				duration = time.Second / 30
			}

			if err = track.WriteSample(media.Sample{Data: nal.Data, Duration: duration}); err != nil {
				return
			}
		}
	}
}

const indexHTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>FFmpeg + Pion WebRTC Stream</title>
  <style>
    body { background: #111; color: #eee; font-family: sans-serif; display: flex; flex-direction: column; align-items: center; padding-top: 40px; }
    video { background: #000; border: 2px solid #444; border-radius: 8px; width: 640px; height: 480px; }
    #status { margin-top: 12px; color: #aaa; }
  </style>
</head>
<body>
  <h2>Live FFmpeg WebRTC Stream</h2>
  <video id="video" autoplay playsinline muted controls></video>
  <div id="status">Connecting...</div>
  <script>
    const status = document.getElementById('status');
    const pc = new RTCPeerConnection();
    pc.addTransceiver('video', { direction: 'recvonly' });

    pc.ontrack = (event) => {
      document.getElementById('video').srcObject = event.streams[0];
    };
    pc.oniceconnectionstatechange = () => {
      status.innerText = 'ICE State: ' + pc.iceConnectionState;
    };

    async function start() {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      const res = await fetch('/offer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pc.localDescription)
      });
      const answer = await res.json();
      await pc.setRemoteDescription(answer);
    }
    start();
  </script>
</body>
</html>`