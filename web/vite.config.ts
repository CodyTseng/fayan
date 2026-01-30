import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // Proxy API requests to the backend during development
      "/users": {
        target: "https://fayan.jumble.social",
        changeOrigin: true,
      },
      "/search": {
        target: "https://fayan.jumble.social",
        changeOrigin: true,
      },
      "/health": {
        target: "https://fayan.jumble.social",
        changeOrigin: true,
      },
    },
  },
});
