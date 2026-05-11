import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const backendHost = process.env.BACKEND_HOST ?? 'localhost:8002'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: true,
    allowedHosts: 'all',
    proxy: {
      '/api': `http://${backendHost}`,
      '/ws': { target: `ws://${backendHost}`, ws: true },
    },
  },
})
