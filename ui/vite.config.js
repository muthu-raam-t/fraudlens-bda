import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // The API runs on the host at :8000; proxying avoids CORS entirely.
      '/api': { target: 'http://localhost:8000', changeOrigin: true,
                rewrite: (p) => p.replace(/^\/api/, '') },
    },
  },
})
