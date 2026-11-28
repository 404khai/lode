import { defineConfig } from "vite";

// `npm run dev` proxies the API to a local lode-lantern on :8080.
export default defineConfig({
  base: "./",
  server: { proxy: { "/api": { target: "http://localhost:8080", changeOrigin: true } } },
  build: { outDir: "dist", emptyOutDir: true },
});
