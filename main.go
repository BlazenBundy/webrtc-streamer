package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/h264reader"
)

// Holds active WebRTC video tracks by Eufy stationSerial
var (
	tracksMu sync.RWMutex
	tracks   = make(map[string]*webrtc.TrackLocalStaticSample)
)

func main() {
	settingEngine := webrtc.SettingEngine{}

	udpListener, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IP{0, 0, 0, 0},
		Port: 8443,
	})
	if err != nil {
		log.Fatalf("Failed to bind UDP port 8443: %v", err)
	}
	settingEngine.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udpListener))

	publicIP := os.Getenv("PUBLIC_IP")
	if publicIP == "" {
		publicIP = "125.63.25.226"
	}
	settingEngine.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeHost)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	// 1. WebRTC Offer Endpoint (Called by Express when Frontend Modal opens)
	http.HandleFunc("/offer", func(w http.ResponseWriter, r *http.Request) {
		station := r.URL.Query().Get("station")
		if station == "" {
			http.Error(w, "Missing station query param", http.StatusBadRequest)
			return
		}

		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		peerConnection, err := api.NewPeerConnection(webrtc.Configuration{
			ICEServers: []webrtc.ICEServer{
				{URLs: []string{"stun:stun.l.google.com:19302"}},
			},
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		videoTrack, err := webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264},
			"video",
			"eufy-"+station,
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

		go func() {
			rtcpBuf := make([]byte, 1500)
			for {
				if _, _, rtcpErr := rtpSender.Read(rtcpBuf); rtcpErr != nil {
					return
				}
			}
		}()

		// Register track for this stationSerial so /ingest can write to it
		tracksMu.Lock()
		tracks[station] = videoTrack
		tracksMu.Unlock()

		peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
			log.Printf("[%s] ICE State: %s", station, state.String())
			if state == webrtc.ICEConnectionStateDisconnected ||
				state == webrtc.ICEConnectionStateFailed ||
				state == webrtc.ICEConnectionStateClosed {
				tracksMu.Lock()
				delete(tracks, station)
				tracksMu.Unlock()
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

	// 2. Raw HEVC Ingest Endpoint (Express pipes Eufy `videostream` straight here)
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		station := r.URL.Query().Get("station")
		if station == "" {
			http.Error(w, "Missing station query param", http.StatusBadRequest)
			return
		}

		log.Printf("[%s] Receiving raw HEVC stream from Express, starting FFmpeg...", station)

		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-f", "hevc",
			"-i", "pipe:0", // Read raw Eufy HEVC from HTTP request body
			"-vf", "scale=1280:-2",
			"-c:v", "libx264",
			"-pix_fmt", "yuv420p",
			"-profile:v", "baseline",
			"-level:v", "3.1",
			"-preset", "ultrafast",
			"-tune", "zerolatency",
			"-g", "15",
			"-r", "15",
			"-f", "h264",
			"pipe:1",
		)

		cmd.Stdin = r.Body
		cmd.Stderr = os.Stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := cmd.Start(); err != nil {
			log.Printf("[%s] FFmpeg start error: %v", station, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		h264, err := h264reader.NewReader(stdout)
		if err != nil {
			log.Printf("[%s] H264 reader error: %v", station, err)
			return
		}
		sentFirstFrame := false
		for {
			nal, err := h264.NextNAL()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("[%s] NAL read error: %v", station, err)
				break
			}

			if !sentFirstFrame {
				log.Printf("[%s] SUCCESS: First H.264 NAL unit produced by FFmpeg (Type: %d, Size: %d bytes)", station, nal.UnitType, len(nal.Data))
				sentFirstFrame = true
			}

			isFrame := nal.UnitType == h264reader.NalUnitTypeCodedSliceNonIdr ||
				nal.UnitType == h264reader.NalUnitTypeCodedSliceIdr

			var duration time.Duration
			if isFrame {
				duration = time.Second / 15
			}

			tracksMu.RLock()
			track := tracks[station]
			tracksMu.RUnlock()

			if track != nil {
				_ = track.WriteSample(media.Sample{Data: nal.Data, Duration: duration})
			}
		}

		_ = cmd.Wait()
		log.Printf("[%s] Ingest stream ended", station)
		w.WriteHeader(http.StatusOK)
	})

	fmt.Println("Streaming server listening on :8080 (Internal HTTP) and :8443/udp (Public WebRTC)")
	log.Fatal(http.ListenAndServe(":8080", nil))
}