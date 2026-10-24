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
	"path/filepath"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/h264reader"
)

// Holds active WebRTC video tracks by Eufy stationSerial
var (
	tracksMu sync.RWMutex
	tracks   = make(map[string][]*webrtc.TrackLocalStaticSample)
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
	settingEngine.SetNAT1To1IPs([]string{"192.168.1.39"}, webrtc.ICECandidateTypeHost)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	// =========================================================================
	// 1. PRIMARY SECURITY CAMERA ROUTES (Intact & Isolated)
	// =========================================================================

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
			ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
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

		tracksMu.Lock()
		tracks[station] = append(tracks[station], videoTrack)
		tracksMu.Unlock()

		peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
			log.Printf("[%s] ICE State: %s", station, state.String())
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

	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		station := r.URL.Query().Get("station")
		if station == "" {
			http.Error(w, "Missing station query param", http.StatusBadRequest)
			return
		}

		inputCodec := r.URL.Query().Get("codec")
		if inputCodec != "h264" {
			inputCodec = "hevc"
		}

		log.Printf("[%s] Receiving raw %s stream from Express, starting FFmpeg...", station, inputCodec)

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
			activeTracks := append([]*webrtc.TrackLocalStaticSample{}, tracks[station]...)
			tracksMu.RUnlock()

			for _, track := range activeTracks {
				_ = track.WriteSample(media.Sample{Data: nal.Data, Duration: duration})
			}
		}

		_ = cmd.Wait()
		log.Printf("[%s] Ingest stream ended", station)
		w.WriteHeader(http.StatusOK)
	})

	// =========================================================================
	// 2. EXPANDED MEDIA ENGINE ROUTES (Independent & QSV Accelerated)
	// =========================================================================

	// AI Snapshot Extractor: Pass a stream URL, instantly returns a JPEG buffer
	http.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		streamURL := r.URL.Query().Get("url")
		if streamURL == "" {
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
		cmd.Stdout = w
		_ = cmd.Run()
	})

	// Hardware DVR Archiver: Saves an incoming stream directly to rolling HLS chunks on disk
	http.HandleFunc("/archive", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("id")
		if streamID == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		outDir := filepath.Join("/mnt/dvr", streamID)
		_ = os.MkdirAll(outDir, 0755)
		outPath := filepath.Join(outDir, "index.m3u8")

		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-i", "pipe:0",
			"-c:v", "h264_qsv", // Intel QuickSync Hardware Encoding
			"-preset", "veryfast",
			"-f", "hls",
			"-hls_time", "10",
			"-hls_list_size", "100", // Keep last 100 chunks
			"-hls_flags", "delete_segments",
			outPath,
		)
		cmd.Stdin = r.Body
		go func() {
			if err := cmd.Run(); err != nil {
				log.Printf("[DVR] Archiver stopped for %s: %v", streamID, err)
			}
		}()
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Archiving started for %s", streamID)
	})

	// Heavy Transcoder: Downscales heavy 4K/RTSP streams to 720p using QSV
	http.HandleFunc("/transcode", func(w http.ResponseWriter, r *http.Request) {
		streamURL := r.URL.Query().Get("url")
		if streamURL == "" {
			http.Error(w, "Missing url parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "video/mp2t")
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-hwaccel", "qsv", // Hardware Decode
			"-i", streamURL,
			"-vf", "scale_qsv=1280:-2", // Hardware Scale
			"-c:v", "h264_qsv",         // Hardware Encode
			"-b:v", "2000k",
			"-c:a", "aac",
			"-f", "mpegts",
			"pipe:1",
		)
		cmd.Stdout = w
		_ = cmd.Run()
	})

	// Timelapse Generator: Stitches standard chunks into a hyper-lapse
	http.HandleFunc("/timelapse", func(w http.ResponseWriter, r *http.Request) {
		dateStr := r.URL.Query().Get("date") // e.g. 2026-10-24
		if dateStr == "" {
			http.Error(w, "Missing date parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "video/mp4")
		inputPath := fmt.Sprintf("/mnt/dvr/camera1/%s_*.ts", dateStr) // Glob pattern
		
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-pattern_type", "glob",
			"-i", inputPath,
			"-filter:v", "setpts=0.02*PTS", // Speed up 50x
			"-an", // Drop audio
			"-c:v", "h264_qsv",
			"-f", "mp4",
			"-movflags", "frag_keyframe+empty_moov",
			"pipe:1",
		)
		cmd.Stdout = w
		_ = cmd.Run()
	})

	// Audio Intelligence: Extracts a clean 16kHz WAV for Whisper/Transcription pipelines
	http.HandleFunc("/transcribe", func(w http.ResponseWriter, r *http.Request) {
		streamURL := r.URL.Query().Get("url")
		if streamURL == "" {
			http.Error(w, "Missing url parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "audio/wav")
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-i", streamURL,
			"-vn", // Drop video
			"-acodec", "pcm_s16le",
			"-ar", "16000",
			"-ac", "1",
			"-f", "wav",
			"pipe:1",
		)
		cmd.Stdout = w
		_ = cmd.Run()
	})

	fmt.Println("Streaming server listening on :8080 (Internal HTTP) and :8443/udp (Public WebRTC)")
	log.Fatal(http.ListenAndServe(":8080", nil))
}