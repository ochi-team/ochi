import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';
import devtools from 'solid-devtools/vite';

export default defineConfig({
  plugins: [devtools(), solidPlugin(), tailwindcss()],
  server: {
    port: 3000,
    proxy: {
      '/query': 'http://127.0.0.1:9014',
    },
  },
  build: {
    target: 'esnext',
  },
});
