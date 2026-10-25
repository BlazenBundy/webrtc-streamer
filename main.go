package main

import (
	"bytes"
	"context"
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
	tracksMu      sync.RWMutex
	tracks        = make(map[string][]*webrtc.TrackLocalStaticSample)
	lastKeyframes = make(map[string][]byte) // Instant-on IDR keyframe cache per station
	webrtcAPI     *webrtc.API
)

func main() {
	log.Println("[SYSTEM] Initializing MS-01 Media Engine...")
	settingEngine := webrtc.SettingEngine{}

	udpListener, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.ParseIP("::"),
		Port: 8443,
	})
	if err != nil {
		log.Fatalf("[SYSTEM-FATAL] Failed to bind UDP port 8443: %v", err)
	}

	log.Println("[SYSTEM] WebRTC UDP Listener bound to 0.0.0.0:8443")
	settingEngine.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udpListener))
	settingEngine.SetNAT1To1IPs([]string{"192.168.1.39"}, webrtc.ICECandidateTypeHost)

	webrtcAPI = webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	// =========================================================================
	// 1. PRIMARY SECURITY CAMERA ROUTES
	// =========================================================================

	http.HandleFunc("/offer", func(w http.ResponseWriter, r *http.Request) {
		station := r.URL.Query().Get("station")
		log.Printf("[WEBRTC-OFFER] Incoming WebRTC connection request for station: '%s'", station)

		if station == "" {
			http.Error(w, "Missing station query param", http.StatusBadRequest)
			return
		}

		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			log.Printf("[WEBRTC-OFFER] [%s] Error decoding SDP offer: %v", station, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		peerConnection, err := webrtcAPI.NewPeerConnection(webrtc.Configuration{})
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

			// Instant-On: Push cached SPS+PPS+IDR keyframe the millisecond ICE connects
			if state == webrtc.ICEConnectionStateConnected {
				tracksMu.RLock()
				cachedIDR := lastKeyframes[station]
				tracksMu.RUnlock()
				if len(cachedIDR) > 0 {
					_ = videoTrack.WriteSample(media.Sample{
						Data:     cachedIDR,
						Duration: time.Second / 15,
					})
				}
			}

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
			http.Error(w, "Missing station query param", http.StatusBadRequest)
			return
		}

		inputCodec := r.URL.Query().Get("codec")
		if inputCodec != "h264" {
			inputCodec = "hevc"
		}

		startTime := time.Now()
		log.Printf("[INGEST] [%s] Receiving stream, codec: %s. Starting ultra-low-latency FFmpeg...", station, inputCodec)

		// Ultra-low-latency FFmpeg pipeline:
		// -probesize 32768 & -analyzeduration 0 eliminate the 5-6s startup buffer
		// -loglevel error silences the thousands of swscaler warnings
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-hide_banner",
			"-loglevel", "error",
			"-fflags", "nobuffer+fastseek+flush_packets",
			"-flags", "low_delay",
			"-probesize", "32768",
			"-analyzeduration", "0",
			"-f", inputCodec,
			"-i", "pipe:0",
			"-reconnect", "1",
			"-reconnect_stream_timeout", "2",
			"-vf", "scale=1280:-2:flags=fast_bilinear",
			"-c:v", "libx264",
			"-pix_fmt", "yuv420p",
			"-profile:v", "baseline",
			"-level:v", "3.1",
			"-preset", "ultrafast",
			"-tune", "zerolatency",
			"-threads", "1",
			"-b:v", "1500k",
			"-maxrate", "1500k",
			"-bufsize", "750k",
			"-g", "15",
			"-keyint_min", "15",
			"-sc_threshold", "0",
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
		var spsCache []byte
		var ppsCache []byte
		var frameBuffer bytes.Buffer

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

			if nal.UnitType == h264reader.NalUnitTypeSPS {
				spsCache = append([]byte{}, nal.Data...)
				continue
			}
			if nal.UnitType == h264reader.NalUnitTypePPS {
				ppsCache = append([]byte{}, nal.Data...)
				continue
			}

			isIDR := nal.UnitType == h264reader.NalUnitTypeCodedSliceIdr
			isNonIDR := nal.UnitType == h264reader.NalUnitTypeCodedSliceNonIdr

			if isIDR || isNonIDR {
				if !sentFirstFrame {
					log.Printf("[INGEST] [%s] SUCCESS: First H.264 Frame generated in %v!", station, time.Since(startTime))
					sentFirstFrame = true
				}

				frameBuffer.Reset()

				if isIDR {
					if len(spsCache) > 0 {
						frameBuffer.Write([]byte{0x00, 0x00, 0x00, 0x01})
						frameBuffer.Write(spsCache)
					}
					if len(ppsCache) > 0 {
						frameBuffer.Write([]byte{0x00, 0x00, 0x00, 0x01})
						frameBuffer.Write(ppsCache)
					}
				}

				frameBuffer.Write([]byte{0x00, 0x00, 0x00, 0x01})
				frameBuffer.Write(nal.Data)

				payload := append([]byte{}, frameBuffer.Bytes()...)
				duration := time.Second / 15

				tracksMu.Lock()
				if isIDR {
					lastKeyframes[station] = payload
				}
				activeTracks := append([]*webrtc.TrackLocalStaticSample{}, tracks[station]...)
				tracksMu.Unlock()

				for _, track := range activeTracks {
					_ = track.WriteSample(media.Sample{Data: payload, Duration: duration})
				}
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
		if streamURL == "" {
			http.Error(w, "Missing url parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "image/jpeg")
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-hide_banner", "-loglevel", "error",
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
			log.Printf("[SNAPSHOT] FFmpeg execution failed: %v | %s", err, stderrBuf.String())
		}
	})

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
			"-hide_banner", "-loglevel", "error",
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
			if err := cmd.Run(); err != nil {
				log.Printf("[ARCHIVE] [%s] Error: %v | %s", streamID, err, stderrBuf.String())
			}
		}()

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Archiving started for %s", streamID)
	})

	http.HandleFunc("/transcode", func(w http.ResponseWriter, r *http.Request) {
		streamURL := r.URL.Query().Get("url")
		if streamURL == "" {
			http.Error(w, "Missing url parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "video/mp4")
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-hide_banner", "-loglevel", "error",
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
			"-f", "mp4",
			"-movflags", "frag_keyframe+empty_moov+default_base_moof",
			"pipe:1",
		)

		var stderrBuf bytes.Buffer
		cmd.Stdout = w
		cmd.Stderr = &stderrBuf
		_ = cmd.Run()
	})

	http.HandleFunc("/timelapse", func(w http.ResponseWriter, r *http.Request) {
		dateStr := r.URL.Query().Get("date")
		if dateStr == "" {
			http.Error(w, "Missing date parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "video/mp4")
		inputPath := fmt.Sprintf("/mnt/dvr/camera1/%s_*.ts", dateStr)

		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-hide_banner", "-loglevel", "error",
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
		_ = cmd.Run()
	})

	http.HandleFunc("/transcribe", func(w http.ResponseWriter, r *http.Request) {
		streamURL := r.URL.Query().Get("url")
		if streamURL == "" {
			http.Error(w, "Missing url parameter", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "audio/wav")
		cmd := exec.CommandContext(r.Context(), "ffmpeg",
			"-hide_banner", "-loglevel", "error",
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
		_ = cmd.Run()
	})

	http.HandleFunc("/transcode-webrtc", handleWebRTC)

	log.Println("[SYSTEM] HTTP routing initialized. Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleWebRTC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	streamURL := r.URL.Query().Get("url")
	if streamURL == "" {
		http.Error(w, "Missing 'url' query parameter", http.StatusBadRequest)
		return
	}

	var offer webrtc.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	peerConnection, err := webrtcAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	videoTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264},
		"video",
		"ms01-qsv-stream",
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

	ctx, cancel := context.WithCancel(context.Background())

	peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("[WEBRTC] ICE State changed: %s", state.String())
		if state == webrtc.ICEConnectionStateConnected {
			go streamQSVToWebRTC(ctx, streamURL, videoTrack)
		}
		if state == webrtc.ICEConnectionStateDisconnected ||
			state == webrtc.ICEConnectionStateFailed ||
			state == webrtc.ICEConnectionStateClosed {
			cancel()
			_ = peerConnection.Close()
		}
	})

	if err := peerConnection.SetRemoteDescription(offer); err != nil {
		cancel()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	answer, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		cancel()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)
	if err := peerConnection.SetLocalDescription(answer); err != nil {
		cancel()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	<-gatherComplete

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(*peerConnection.LocalDescription())
}

func streamQSVToWebRTC(ctx context.Context, streamURL string, videoTrack *webrtc.TrackLocalStaticSample) {
	log.Printf("[WEBRTC-QSV] Starting real-time QSV pipeline for: %s", streamURL)

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-re",
		"-init_hw_device", "vaapi=va:/dev/dri/renderD128",
		"-init_hw_device", "qsv=qs@va",
		"-filter_hw_device", "qs",
		"-hwaccel", "qsv",
		"-hwaccel_output_format", "qsv",
		"-c:v", "h264_qsv",
		"-i", streamURL,
		"-vf", "scale_qsv=w=1280:h=-1",
		"-c:v", "h264_qsv",
		"-profile:v", "baseline",
		"-bf", "0",
		"-g", "30",
		"-r", "30",
		"-b:v", "2000k",
		"-an",
		"-f", "h264",
		"pipe:1",
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("[WEBRTC-QSV] Pipe error: %v", err)
		return
	}

	if err := cmd.Start(); err != nil {
		log.Printf("[WEBRTC-QSV] FFmpeg start error: %v", err)
		return
	}

	h264, err := h264reader.NewReader(stdout)
	if err != nil {
		log.Printf("[WEBRTC-QSV] H264Reader error: %v", err)
		return
	}

	frameDuration := time.Second / 30
	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()

	var frameBuffer bytes.Buffer

	for {
		select {
		case <-ctx.Done():
			log.Printf("[WEBRTC-QSV] Stopping QSV WebRTC stream")
			return
		default:
			nal, err := h264.NextNAL()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Printf("[WEBRTC-QSV] NAL read error: %v", err)
				return
			}

			frameBuffer.Write([]byte{0x00, 0x00, 0x00, 0x01})
			frameBuffer.Write(nal.Data)

			if nal.UnitType == h264reader.NalUnitTypeCodedSliceIdr ||
				nal.UnitType == h264reader.NalUnitTypeCodedSliceNonIdr {

				<-ticker.C
				_ = videoTrack.WriteSample(media.Sample{
					Data:     frameBuffer.Bytes(),
					Duration: frameDuration,
				})
				frameBuffer.Reset()
			}
		}
	}
}