import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
        // SSE needs these settings
        configure: (proxy) => {
          proxy.on("proxyReq", (_, req) => {
            if (req.url?.includes("/stream")) {
              (req as unknown as Record<string, unknown>).timeout = 0;
            }
          });
        },
      },
    },
  },
});
