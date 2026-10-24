package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/h264reader"
)

var (
	tracksMu sync.RWMutex
	tracks   = make(map[string][]*webrtc.TrackLocalStaticSample)
)

func main() {
	log.Println("[SYSTEM] Initializing MS-01 Media Engine...")
	settingEngine := webrtc.SettingEngine{}

	udpListener, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IP{0, 0, 0, 0},
		Port: 8443,
	})
	if err != nil {
		log.Fatalf("[SYSTEM-FATAL] Failed to bind UDP port 8443: %v", err)
	}
	
	log.Println("[SYSTEM] WebRTC UDP Listener bound to 0.0.0.0:8443")
	settingEngine.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udpListener))
	settingEngine.SetNAT1To1IPs([]string{"192.168.1.39"}, webrtc.ICECandidateTypeHost)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	// =========================================================================
	// 1. PRIMARY SECURITY CAMERA ROUTES
	// =========================================================================

	http.HandleFunc("/offer", func(w http.ResponseWriter, r *http.Request) {
		station := r.URL.Query().Get("station")
		log.Printf("[WEBRTC-OFFER] Incoming WebRTC connection request for station: '%s'", station)
		
		if station == "" {
			log.Println("[WEBRTC-OFFER] Error: Missing station query param")
			http.Error(w, "Missing station query param", http.StatusBadRequest)
			return
		}

		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			log.Printf("[WEBRTC-OFFER] [%s] Error decoding SDP offer: %v", station, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		peerConnection, err := api.NewPeerConnection(webrtc.Configuration{
			ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
		})
		if err != nil {
			log.Printf("[WEBRTC-OFFER] [%s] Error creating PeerConnection: %v", station, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		videoTrack, err := webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264},
			"video",
			"eufy-"+station,
		)
		if err != nil {
			log.Printf("[WEBRTC-OFFER] [%s] Error creating track: %v", station, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rtpSender, err := peerConnection.AddTrack(videoTrack)
		if err != nil {
			log.Printf("[WEBRTC-OFFER] [%s] Error adding track: %v", station, err)
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

		tracksMu.Lock()
		tracks[station] = append(tracks[station], videoTrack)
		viewerCount := len(tracks[station])
		tracksMu.Unlock()
		
		log.Printf("[WEBRTC-OFFER] [%s] Track added. Active viewers: %d", station, viewerCount)

		peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
			log.Printf("[WEBRTC-ICE] [%s] State changed to: %s", station, state.String())
			if state == webrtc.ICEConnectionStateDisconnected ||
				state == webrtc.ICEConnectionStateFailed ||
				state == webrtc.ICEConnectionStateClosed {

				tracksMu.Lock()
				activeTracks := tracks[station]
				for i, t := range activeTracks {
					if t == videoTrack {
						tracks[station] = append(activeTracks[:i], activeTracks[i+1:]...)
						break
					}
				}
				newCount := len(tracks[station])
				tracksMu.Unlock()
				
				log.Printf("[WEBRTC-ICE] [%s] Viewer disconnected. Remaining viewers: %d", station, newCount)
				peerConnection.Close()
			}
		})

		if err = peerConnection.SetRemoteDescription(offer); err != nil {
			log.Printf("[WEBRTC-OFFER] [%s] Error setting remote description: %v", station, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		answer, err := peerConnection.CreateAnswer(nil)
		if err != nil {
			log.Printf("[WEBRTC-OFFER] [%s] Error creating answer: %v", station, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		gatherComplete := webrtc.GatheringCompletePromise(peerConnection)
		if err = peerConnection.SetLocalDescription(answer); err != nil {
			log.Printf("[WEBRTC-OFFER] [%s] Error setting local description: %v", station, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		<-gatherComplete

		log.Printf("[WEBRTC-OFFER] [%s] Successfully negotiated WebRTC connection", station)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(peerConnection.LocalDescription())
	})

	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		station := r.URL.Query().Get("station")
		if station == "" {
			log.Println("[INGEST] Error: Missing station query param")
			http.Error(w, "Missing station query param", http.StatusBadRequest)
			return
		}

		inputCodec := r.URL.Query().Get("codec")
		if inputCodec != "h264" {
			inputCodec = "hevc"
		}

		log.Printf("[INGEST] [%s] Receiving stream, codec: %s. Starting internal FFmpeg...", station, inputCodec)

		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-f", inputCodec,
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
			log.Printf("[INGEST] [%s] StdoutPipe error: %v", station, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := cmd.Start(); err != nil {
			log.Printf("[INGEST] [%s] FFmpeg process failed to start: %v", station, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		h264, err := h264reader.NewReader(stdout)
		if err != nil {
			log.Printf("[INGEST] [%s] H264 reader initialization error: %v", station, err)
			return
		}
		
		sentFirstFrame := false
		for {
			nal, err := h264.NextNAL()
			if err == io.EOF {
				log.Printf("[INGEST] [%s] Reached EOF from FFmpeg stdout", station)
				break
			}
			if err != nil {
				log.Printf("[INGEST] [%s] NAL read error: %v", station, err)
				break
			}

			if !sentFirstFrame {
				log.Printf("[INGEST] [%s] SUCCESS: First H.264 NAL unit generated (Type: %d, Size: %d bytes)", station, nal.UnitType, len(nal.Data))
				sentFirstFrame = true
			}

			isFrame := nal.UnitType == h264reader.NalUnitTypeCodedSliceNonIdr ||
				nal.UnitType == h264reader.NalUnitTypeCodedSliceIdr

			var duration time.Duration
			if isFrame {
				duration = time.Second / 15
			}

			tracksMu.RLock()
			activeTracks := append([]*webrtc.TrackLocalStaticSample{}, tracks[station]...)
			tracksMu.RUnlock()

			for _, track := range activeTracks {
				_ = track.WriteSample(media.Sample{Data: nal.Data, Duration: duration})
			}
		}

		err = cmd.Wait()
		if err != nil {
			log.Printf("[INGEST] [%s] FFmpeg exited with error: %v", station, err)
		} else {
			log.Printf("[INGEST] [%s] FFmpeg stream ended cleanly", station)
		}
		w.WriteHeader(http.StatusOK)
	})

	// =========================================================================
	// 2. EXPANDED MEDIA ENGINE ROUTES 
	// =========================================================================

	http.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		streamURL := r.URL.Query().Get("url")
		log.Printf("[SNAPSHOT] Request initiated for URL: %s", streamURL)
		
		if streamURL == "" {
			log.Println("[SNAPSHOT] Error: Missing url parameter")
			http.Error(w, "Missing url parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "image/jpeg")
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-i", streamURL,
			"-vframes", "1",
			"-q:v", "2",
			"-f", "image2",
			"pipe:1",
		)
		
		var stderrBuf bytes.Buffer
		cmd.Stdout = w
		cmd.Stderr = &stderrBuf 
		
		if err := cmd.Run(); err != nil {
			log.Printf("[SNAPSHOT] FFmpeg execution failed. Error: %v", err)
			log.Printf("[SNAPSHOT] FFmpeg Stderr Output:\n%s", stderrBuf.String())
		} else {
			log.Println("[SNAPSHOT] Successfully processed and returned image buffer")
		}
	})

	http.HandleFunc("/archive", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("id")
		log.Printf("[ARCHIVE] DVR recording requested for ID: %s", streamID)
		
		if streamID == "" {
			log.Println("[ARCHIVE] Error: Missing id parameter")
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		outDir := filepath.Join("/mnt/dvr", streamID)
		if err := os.MkdirAll(outDir, 0755); err != nil {
			log.Printf("[ARCHIVE] Error creating directory %s: %v", outDir, err)
		}
		outPath := filepath.Join(outDir, "index.m3u8")

		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-i", "pipe:0",
			"-c:v", "h264_qsv", 
			"-preset", "veryfast",
			"-f", "hls",
			"-hls_time", "10",
			"-hls_list_size", "100", 
			"-hls_flags", "delete_segments",
			outPath,
		)
		cmd.Stdin = r.Body
		
		go func() {
			var stderrBuf bytes.Buffer
			cmd.Stderr = &stderrBuf
			log.Printf("[ARCHIVE] Starting background QSV encoding to %s", outPath)
			
			if err := cmd.Run(); err != nil {
				log.Printf("[ARCHIVE] [%s] Process stopped with error: %v", streamID, err)
				log.Printf("[ARCHIVE] [%s] Stderr Output:\n%s", streamID, stderrBuf.String())
			} else {
				log.Printf("[ARCHIVE] [%s] Process finished gracefully", streamID)
			}
		}()
		
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Archiving started for %s", streamID)
	})

	http.HandleFunc("/transcode", func(w http.ResponseWriter, r *http.Request) {
		streamURL := r.URL.Query().Get("url")
		log.Printf("[TRANSCODE] Request initiated for URL: %s", streamURL)
		
		if streamURL == "" {
			log.Println("[TRANSCODE] Error: Missing url parameter")
			http.Error(w, "Missing url parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "video/mp2t")
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-init_hw_device", "vaapi=va:/dev/dri/renderD128",
			"-init_hw_device", "qsv=qs@va",
			"-filter_hw_device", "qs",
			"-hwaccel", "qsv",
			"-hwaccel_output_format", "qsv",
			"-c:v", "h264_qsv",
			"-i", streamURL,
			"-vf", "scale_qsv=w=1280:h=-1",
			"-c:v", "h264_qsv",
			"-b:v", "2000k",
			"-c:a", "aac",
			"-f", "mpegts",
			"pipe:1",
		)
		
		var stderrBuf bytes.Buffer
		cmd.Stdout = w
		cmd.Stderr = &stderrBuf
		
		if err := cmd.Run(); err != nil {
			log.Printf("[TRANSCODE] Execution failed for %s. Error: %v", streamURL, err)
			log.Printf("[TRANSCODE] Stderr Output:\n%s", stderrBuf.String())
		} else {
			log.Printf("[TRANSCODE] Successfully completed stream proxy for %s", streamURL)
		}
	})

	http.HandleFunc("/timelapse", func(w http.ResponseWriter, r *http.Request) {
		dateStr := r.URL.Query().Get("date") 
		log.Printf("[TIMELAPSE] Generation requested for date: %s", dateStr)
		
		if dateStr == "" {
			log.Println("[TIMELAPSE] Error: Missing date parameter")
			http.Error(w, "Missing date parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "video/mp4")
		inputPath := fmt.Sprintf("/mnt/dvr/camera1/%s_*.ts", dateStr)
		
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-pattern_type", "glob",
			"-i", inputPath,
			"-filter:v", "setpts=0.02*PTS",
			"-an", 
			"-c:v", "h264_qsv",
			"-f", "mp4",
			"-movflags", "frag_keyframe+empty_moov",
			"pipe:1",
		)
		
		var stderrBuf bytes.Buffer
		cmd.Stdout = w
		cmd.Stderr = &stderrBuf
		
		if err := cmd.Run(); err != nil {
			log.Printf("[TIMELAPSE] Execution failed for %s. Error: %v", dateStr, err)
			log.Printf("[TIMELAPSE] Stderr Output:\n%s", stderrBuf.String())
		} else {
			log.Printf("[TIMELAPSE] Successfully generated file for %s", dateStr)
		}
	})

	http.HandleFunc("/transcribe", func(w http.ResponseWriter, r *http.Request) {
		streamURL := r.URL.Query().Get("url")
		log.Printf("[TRANSCRIBE] Audio extraction requested for URL: %s", streamURL)
		
		if streamURL == "" {
			log.Println("[TRANSCRIBE] Error: Missing url parameter")
			http.Error(w, "Missing url parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "audio/wav")
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-i", streamURL,
			"-vn", 
			"-acodec", "pcm_s16le",
			"-ar", "16000",
			"-ac", "1",
			"-f", "wav",
			"pipe:1",
		)
		
		var stderrBuf bytes.Buffer
		cmd.Stdout = w
		cmd.Stderr = &stderrBuf
		
		if err := cmd.Run(); err != nil {
			log.Printf("[TRANSCRIBE] Execution failed for %s. Error: %v", streamURL, err)
			log.Printf("[TRANSCRIBE] Stderr Output:\n%s", stderrBuf.String())
		} else {
			log.Printf("[TRANSCRIBE] Successfully extracted 16kHz audio for %s", streamURL)
		}
	})

	log.Println("[SYSTEM] HTTP routing initialized. Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}