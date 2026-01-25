"use client";

import {
  LiveKitRoom,
  VideoTrack,
  useTracks,
  useRoomContext,
  useConnectionState,
  RoomAudioRenderer,
  useLocalParticipant,
} from "@livekit/components-react";
import { ConnectionState, Track, RoomEvent, ParticipantEvent } from "livekit-client";
import { useCallback, useEffect, useState, useRef } from "react";
import { BrowserDisplay } from "./BrowserDisplay";

interface AvatarRoomProps {
  token: string;
  wsUrl: string;
  roomName: string;
  onDisconnect: () => void;
}

function AvatarDisplay({ isPiP = false, isSpeaking = false }: { isPiP?: boolean; isSpeaking?: boolean }) {
  // Get all video tracks in the room
  const tracks = useTracks([Track.Source.Camera]);

  // Find the avatar's video track (it's the only non-local camera track)
  const avatarTrack = tracks.find(
    (track) => !track.participant.isLocal && track.source === Track.Source.Camera
  );

  if (!avatarTrack) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400 bg-black">
        <div className="text-center">
          <div className="w-20 h-20 mx-auto mb-6 rounded-full bg-white/5 flex items-center justify-center ring-1 ring-white/10">
            <svg className="w-10 h-10 text-white/30 animate-pulse" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
            </svg>
          </div>
          <div className="font-medium text-white/70 mb-2">Waiting for Nora...</div>
          <div className="text-sm text-white/40">The avatar will appear when connected</div>
        </div>
      </div>
    );
  }

  return (
    <div className="relative w-full h-full flex items-center justify-center bg-black">
      <VideoTrack
        trackRef={avatarTrack}
        className={`${isPiP ? 'w-full h-full object-cover' : 'max-w-full max-h-full object-contain'}`}
      />
    </div>
  );
}

function ConnectionStatus() {
  const connectionState = useConnectionState();

  const statusConfig: Record<ConnectionState, { color: string; pulse: boolean; label: string }> = {
    [ConnectionState.Connected]: { color: "bg-emerald-400", pulse: false, label: "Connected" },
    [ConnectionState.Connecting]: { color: "bg-amber-400", pulse: true, label: "Connecting" },
    [ConnectionState.Disconnected]: { color: "bg-red-400", pulse: false, label: "Disconnected" },
    [ConnectionState.Reconnecting]: { color: "bg-amber-400", pulse: true, label: "Reconnecting" },
    [ConnectionState.SignalReconnecting]: { color: "bg-amber-400", pulse: true, label: "Reconnecting" },
  };

  const config = statusConfig[connectionState] || { color: "bg-gray-400", pulse: false, label: "Unknown" };

  return (
    <div className="flex items-center gap-2 text-xs px-3 py-1.5 bg-white/5 rounded-full backdrop-blur-md border border-white/10">
      <div className="relative">
        <div className={`w-1.5 h-1.5 rounded-full ${config.color}`} />
        {config.pulse && (
          <div className={`absolute inset-0 w-1.5 h-1.5 rounded-full ${config.color} animate-ping`} />
        )}
      </div>
      <span className="text-white/60 font-medium">{config.label}</span>
    </div>
  );
}

function MicrophoneToggle() {
  const { localParticipant } = useLocalParticipant();
  const [isMuted, setIsMuted] = useState(false);

  const toggleMicrophone = useCallback(async () => {
    if (!localParticipant) return;

    await localParticipant.setMicrophoneEnabled(isMuted);
    setIsMuted(!isMuted);
  }, [localParticipant, isMuted]);

  // Enable microphone on mount
  useEffect(() => {
    if (localParticipant) {
      localParticipant.setMicrophoneEnabled(true);
    }
  }, [localParticipant]);

  return (
    <button
      onClick={toggleMicrophone}
      className={`flex items-center gap-2.5 px-5 py-3 rounded-full font-medium transition-all duration-300 backdrop-blur-md ${
        isMuted
          ? "bg-red-500/10 hover:bg-red-500/20 text-red-400 border border-red-500/30"
          : "bg-white/5 hover:bg-white/10 text-white/80 border border-white/10"
      }`}
    >
      {isMuted ? (
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5.586 15H4a1 1 0 01-1-1v-4a1 1 0 011-1h1.586l4.707-4.707C10.923 3.663 12 4.109 12 5v14c0 .891-1.077 1.337-1.707.707L5.586 15z" />
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2" />
        </svg>
      ) : (
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11a7 7 0 01-7 7m0 0a7 7 0 01-7-7m7 7v4m0 0H8m4 0h4m-4-8a3 3 0 01-3-3V5a3 3 0 116 0v6a3 3 0 01-3 3z" />
        </svg>
      )}
      {isMuted ? "Muted" : "Listening"}
    </button>
  );
}

function RoomContent({ onDisconnect }: { onDisconnect: () => void }) {
  const room = useRoomContext();

  // Browser state - separate VNC credentials from active browsing state
  const [vncHostname, setVncHostname] = useState<string>("");
  const [vncPassword, setVncPassword] = useState<string>("");
  const [hasVncCredentials, setHasVncCredentials] = useState(false);
  const [browserActive, setBrowserActive] = useState(false);

  // Speaking state for glow effect
  const [isSpeaking, setIsSpeaking] = useState(false);
  const speakingTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Track when avatar (remote participant) is speaking
  useEffect(() => {
    const checkSpeaking = () => {
      // Get the first remote participant (Nora/avatar)
      const remoteParticipants = Array.from(room.remoteParticipants.values());
      if (remoteParticipants.length > 0) {
        const avatarParticipant = remoteParticipants[0];

        const handleSpeakingChanged = (speaking: boolean) => {
          if (speaking) {
            // Clear any pending timeout
            if (speakingTimeoutRef.current) {
              clearTimeout(speakingTimeoutRef.current);
              speakingTimeoutRef.current = null;
            }
            setIsSpeaking(true);
          } else {
            // Add small delay before turning off glow for smoother effect
            speakingTimeoutRef.current = setTimeout(() => {
              setIsSpeaking(false);
            }, 300);
          }
        };

        avatarParticipant.on(ParticipantEvent.IsSpeakingChanged, handleSpeakingChanged);

        // Check initial state
        if (avatarParticipant.isSpeaking) {
          setIsSpeaking(true);
        }

        return () => {
          avatarParticipant.off(ParticipantEvent.IsSpeakingChanged, handleSpeakingChanged);
          if (speakingTimeoutRef.current) {
            clearTimeout(speakingTimeoutRef.current);
          }
        };
      }
    };

    // Initial check
    const cleanup = checkSpeaking();

    // Also listen for new participants joining
    const handleParticipantConnected = () => {
      checkSpeaking();
    };

    room.on(RoomEvent.ParticipantConnected, handleParticipantConnected);

    return () => {
      cleanup?.();
      room.off(RoomEvent.ParticipantConnected, handleParticipantConnected);
    };
  }, [room]);

  // Listen for browser messages from agent via data channel
  useEffect(() => {
    const handleData = (payload: Uint8Array) => {
      try {
        const data = JSON.parse(new TextDecoder().decode(payload));
        console.log("[DEBUG] Received data message:", data.type);

        if (data.type === 'browser_ready') {
          setVncHostname(data.hostname);
          setVncPassword(data.password);
          setHasVncCredentials(true);
        } else if (data.type === 'browser_task_started') {
          setBrowserActive(true);
        } else if (data.type === 'browser_task_completed') {
          setBrowserActive(false);
        }
      } catch (e) {
        console.error("Failed to parse data message:", e);
      }
    };

    room.on(RoomEvent.DataReceived, handleData);
    return () => {
      room.off(RoomEvent.DataReceived, handleData);
    };
  }, [room]);

  const handleDisconnect = useCallback(() => {
    room.disconnect();
    onDisconnect();
  }, [room, onDisconnect]);

  return (
    <div className="flex flex-col h-screen bg-black">
      {/* Floating header - minimal, top-right */}
      <header className="absolute top-4 right-4 z-20 flex items-center gap-3">
        <ConnectionStatus />
        {browserActive && (
          <span className="px-2.5 py-1.5 text-xs font-medium bg-blue-500/10 text-blue-400 rounded-full border border-blue-500/20 flex items-center gap-1.5 backdrop-blur-md">
            <span className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse" />
            Browsing
          </span>
        )}
      </header>

      {/* Main content - Avatar centered with glow effect */}
      <main className="flex-1 flex items-center justify-center p-4 md:p-8 overflow-hidden relative">
        {/* Browser - full screen when active */}
        {browserActive && hasVncCredentials && (
          <div className="absolute inset-4 md:inset-8 z-0 transition-all duration-500 ease-out rounded-3xl overflow-hidden">
            <BrowserDisplay
              hostname={vncHostname}
              password={vncPassword}
              isVisible={true}
            />
          </div>
        )}

        {/* Avatar container with speaking glow */}
        <div
          className={`transition-all duration-500 ease-out ${
            browserActive
              ? "absolute bottom-20 right-4 md:bottom-24 md:right-8 w-[180px] h-[180px] md:w-[240px] md:h-[240px] z-10"
              : "w-full max-w-3xl aspect-square md:aspect-video"
          }`}
        >
          <div
            className={`
              relative w-full h-full rounded-3xl overflow-hidden
              transition-all duration-300 ease-out
              ${isSpeaking
                ? 'speaking-glow ring-2 ring-violet-400/50'
                : 'idle-breathe ring-1 ring-white/5'
              }
            `}
          >
            <AvatarDisplay isPiP={browserActive} isSpeaking={isSpeaking} />
          </div>
        </div>
      </main>

      {/* Floating controls - bottom center */}
      <footer className="absolute bottom-6 left-1/2 -translate-x-1/2 z-20 flex items-center gap-3">
        <MicrophoneToggle />
        <button
          onClick={handleDisconnect}
          className="flex items-center gap-2 px-5 py-3 bg-white/5 hover:bg-white/10 text-white/60 hover:text-white/90 rounded-full font-medium transition-all duration-300 backdrop-blur-md border border-white/10"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
          </svg>
          Leave
        </button>
      </footer>

      {/* Audio renderer - plays remote audio tracks */}
      <RoomAudioRenderer />
    </div>
  );
}

export default function AvatarRoom({
  token,
  wsUrl,
  roomName,
  onDisconnect,
}: AvatarRoomProps) {
  return (
    <LiveKitRoom
      token={token}
      serverUrl={wsUrl}
      connect={true}
      audio={true}
      video={false}
      onDisconnected={onDisconnect}
    >
      <RoomContent onDisconnect={onDisconnect} />
    </LiveKitRoom>
  );
}
