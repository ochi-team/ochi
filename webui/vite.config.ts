import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import solid from "@solidjs/vite-plugin";

export default defineConfig(() => {
  return {
    envPrefix: ["VITE_", "OCHI_"],
    resolve: { tsconfigPaths: true },
    // Turnkey client mode: no index.html and no mount file — the plugin generates
    // the entries around src/App.tsx (wrapped in src/Document.tsx) and `vite build`
    // prerenders the shell into a purely static dist/client.
    plugins: [solid({ start: true, diagnostics: true }), tailwindcss()],
    server: {
      port: 3000,
    },
    build: {
      target: "esnext",
      assetsInlineLimit: 0,
    },
  };
});
