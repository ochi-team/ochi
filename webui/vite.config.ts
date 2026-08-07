import tailwindcss from '@tailwindcss/vite';
import { defineConfig, loadEnv } from 'vite';
import solidPlugin from 'vite-plugin-solid';
import devtools from 'solid-devtools/vite';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '.', 'OCHI');
  const host = env.OCHI_HOST || 'http://127.0.0.1:9014';

  return {
    envPrefix: 'OCHI_',
    plugins: [devtools(), solidPlugin(), tailwindcss()],
    server: {
      port: 3000,
      proxy: {
        '/query': host,
      },
    },
    build: {
      target: 'esnext',
    },
  };
});
