package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"

	"github.com/pion/webrtc/v4"
)

func main() {
	// 1. Verify FFmpeg is installed and reachable
	versionCmd := exec.Command("ffmpeg", "-version")
	if out, err := versionCmd.Output(); err != nil {
		log.Fatalf("FFmpeg check failed: %v", err)
	} else {
		log.Printf("FFmpeg detected: %s", string(out[:45]))
	}

	// 2. Health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK - Go + FFmpeg + Pion WebRTC Ready\n"))
	})

	// 3. WebRTC Signaling Endpoint (Receives Browser SDP Offer, Returns Answer)
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

		peerConnection, err := webrtc.NewPeerConnection(webrtc.Configuration{
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
			"ffmpeg-stream",
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if _, err = peerConnection.AddTrack(videoTrack); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

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

	fmt.Println("Streaming server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}