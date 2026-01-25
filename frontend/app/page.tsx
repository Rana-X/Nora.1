"use client";

import { useState, useCallback } from "react";
import dynamic from "next/dynamic";

// Dynamically import AvatarRoom to avoid SSR issues with LiveKit
const AvatarRoom = dynamic(() => import("@/components/AvatarRoom"), {
  ssr: false,
});

interface RoomCredentials {
  token: string;
  wsUrl: string;
  roomName: string;
}

export default function Home() {
  const [isConnecting, setIsConnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [roomCredentials, setRoomCredentials] = useState<RoomCredentials | null>(
    null
  );

  const joinRoom = useCallback(async () => {
    setIsConnecting(true);
    setError(null);

    try {
      // Generate a unique room name and participant identity
      const roomName = `nora-room-${Date.now()}`;
      const participantName = `user-${Math.random().toString(36).substring(7)}`;

      const response = await fetch("/api/token", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          roomName,
          participantName,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to get token");
      }

      const { token, wsUrl } = await response.json();

      setRoomCredentials({
        token,
        wsUrl,
        roomName,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "An error occurred");
    } finally {
      setIsConnecting(false);
    }
  }, []);

  const handleDisconnect = useCallback(() => {
    setRoomCredentials(null);
  }, []);

  // If connected to a room, show the avatar room
  if (roomCredentials) {
    return (
      <AvatarRoom
        token={roomCredentials.token}
        wsUrl={roomCredentials.wsUrl}
        roomName={roomCredentials.roomName}
        onDisconnect={handleDisconnect}
      />
    );
  }

  // Landing page - minimal
  return (
    <div className="flex min-h-screen flex-col items-center justify-center bg-black">
      {/* NORA title in sky blue */}
      <h1 className="text-7xl md:text-8xl font-bold text-sky-400 tracking-wider mb-16">
        NORA
      </h1>

      {/* Error message */}
      {error && (
        <div className="mb-8 px-4 py-2 text-red-400 text-sm">
          {error}
        </div>
      )}

      {/* Start button */}
      <button
        onClick={joinRoom}
        disabled={isConnecting}
        className={`px-12 py-4 text-xl font-medium rounded-full transition-all duration-300 ${
          isConnecting
            ? "text-white/30 cursor-not-allowed"
            : "text-sky-400 hover:text-sky-300 hover:bg-sky-400/10 border border-sky-400/30 hover:border-sky-400/50"
        }`}
      >
        {isConnecting ? "Connecting..." : "Start"}
      </button>
    </div>
  );
}
