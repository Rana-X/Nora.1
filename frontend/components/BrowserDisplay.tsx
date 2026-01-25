"use client";

import { ComputerDisplay } from "orgo-vnc";
import { useState } from "react";

interface BrowserDisplayProps {
  hostname: string;
  password: string;
  isVisible: boolean;
}

export function BrowserDisplay({ hostname, password, isVisible }: BrowserDisplayProps) {
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Debug: Log the credentials we received
  console.log("[BrowserDisplay] Rendering with:", {
    hostname: hostname ? `${hostname.slice(0, 20)}...` : "none",
    hasPassword: !!password,
    isVisible
  });

  if (!isVisible) {
    return null;
  }

  if (!hostname || !password) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-800 text-gray-400 rounded-2xl">
        <div className="text-center">
          <div className="text-lg mb-2">Waiting for browser...</div>
          <div className="text-sm text-yellow-400">
            No VNC credentials received yet
          </div>
          <div className="text-xs mt-2 text-gray-500">
            hostname: {hostname ? "✓" : "✗"} | password: {password ? "✓" : "✗"}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full rounded-2xl overflow-hidden bg-gray-900 relative">
      {/* Connection status overlay */}
      {!connected && !error && (
        <div className="absolute inset-0 flex items-center justify-center bg-gray-900/80 z-10">
          <div className="text-center text-white">
            <div className="animate-pulse">Connecting to browser...</div>
            <div className="text-xs mt-1 text-gray-400">{hostname.slice(0, 30)}...</div>
          </div>
        </div>
      )}
      {error && (
        <div className="absolute inset-0 flex items-center justify-center bg-red-900/80 z-10">
          <div className="text-center text-white">
            <div className="text-red-300">VNC Error</div>
            <div className="text-xs mt-1">{error}</div>
          </div>
        </div>
      )}
      <ComputerDisplay
        hostname={hostname}
        password={password}
        className="w-full h-full"
        readOnly={true}
        onConnect={() => {
          console.log("[BrowserDisplay] VNC connected!");
          setConnected(true);
          setError(null);
        }}
        onDisconnect={() => {
          console.log("[BrowserDisplay] VNC disconnected");
          setConnected(false);
        }}
        onError={(err: string) => {
          console.error("[BrowserDisplay] VNC error:", err);
          setError(err || "Connection failed");
        }}
      />
    </div>
  );
}
