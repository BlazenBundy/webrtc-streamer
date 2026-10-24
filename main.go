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
	// Updated to hold an array (slice) of tracks for multiple viewers
	tracks = make(map[string][]*webrtc.TrackLocalStaticSample)
)

func main() {
	settingEngine := webrtc.SettingEngine{}

	// Bind the explicit UDP listener to port 8443 for host networking
	udpListener, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IP{0, 0, 0, 0},
		Port: 8443,
	})
	if err != nil {
		log.Fatalf("Failed to bind UDP port 8443: %v", err)
	}
	settingEngine.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udpListener))

	// Explicitly force your server's public IP for browser WebRTC connectivity
	settingEngine.SetNAT1To1IPs([]string{"192.168.1.39"}, webrtc.ICECandidateTypeHost)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	// 1. WebRTC Offer Endpoint
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

		// Append the new viewer's track to the station's slice
		tracksMu.Lock()
		tracks[station] = append(tracks[station], videoTrack)
		tracksMu.Unlock()

		peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
			log.Printf("[%s] ICE State: %s", station, state.String())
			if state == webrtc.ICEConnectionStateDisconnected ||
				state == webrtc.ICEConnectionStateFailed ||
				state == webrtc.ICEConnectionStateClosed {
				
				// Safely remove only this specific viewer's track when they disconnect
				tracksMu.Lock()
				activeTracks := tracks[station]
				for i, t := range activeTracks {
					if t == videoTrack {
						tracks[station] = append(activeTracks[:i], activeTracks[i+1:]...)
						break
					}
				}
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

	// 2. Raw Ingest Endpoint (Dynamically handles H.264 or HEVC)
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		station := r.URL.Query().Get("station")
		if station == "" {
			http.Error(w, "Missing station query param", http.StatusBadRequest)
			return
		}

		// Grab the codec sent from Express (default to hevc if missing)
		inputCodec := r.URL.Query().Get("codec")
		if inputCodec != "h264" {
			inputCodec = "hevc"
		}

		log.Printf("[%s] Receiving raw %s stream from Express, starting FFmpeg...", station, inputCodec)

		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-f", inputCodec, // Dynamically read the input format
			"-i", "pipe:0",
			"-vf", "scale=1280:-2", 
			"-c:v", "libx264",
			"-pix_fmt", "yuv420p",
			"-profile:v", "baseline",
			"-level:v", "3.1",
			"-preset", "ultrafast",
			"-tune", "zerolatency",
			"-threads", "1",
			"-b:v", "1500k",
			"-maxrate", "1500k",
			"-bufsize", "3000k",
			"-g", "15",
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

			// Safely copy the list of active viewers to avoid blocking new connections
			tracksMu.RLock()
			activeTracks := append([]*webrtc.TrackLocalStaticSample{}, tracks[station]...)
			tracksMu.RUnlock()

			// Broadcast the frame to every connected viewer
			for _, track := range activeTracks {
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