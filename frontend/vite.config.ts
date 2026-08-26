/// <reference types="vitest" />
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig(({ mode }) => {
    const env = loadEnv(mode, process.cwd(), '');

    return {
        plugins: [react()],

        define: {
            global: 'globalThis',
        },

        resolve: {
            alias: {
                '@': path.resolve(__dirname, './src'),
                '@components': path.resolve(__dirname, './src/components'),
                '@store': path.resolve(__dirname, './src/store'),
                '@api': path.resolve(__dirname, './src/api'),
                '@pages': path.resolve(__dirname, './src/pages'),
                '@hooks': path.resolve(__dirname, './src/hooks'),
                '@utils': path.resolve(__dirname, './src/utils'),
                '@types': path.resolve(__dirname, './src/types'),
            },
        },

        server: {
            // Use a fixed non-default port so startup fails clearly on a
            // collision instead of silently choosing another port.
            port: 5273,
            strictPort: true,
            // The dev proxy targets localhost services whose security model
            // trusts loopback peers. Exposing this proxy would make remote
            // requests indistinguishable from local ones at the backend.
            host: '127.0.0.1',
            proxy: {
                // Keep the more specific session-aware route before the
                // Python file-processing prefix below.
                '/api/files/search': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                },
                '/api/sessions': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                },
                '/api/workbench': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                },
                // Python-backed panels (file tree / complexity / code
                // insight / API contract) go through the backend, not straight
                // to Python: the sidecar only listens on a Unix socket
                // (ZK_PYTHON_UDS), so a browser cannot reach it directly.
                // zk-server reverse-proxies these prefixes over that socket
                // and answers 503 when the sidecar is down.
                '/api/git': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                },
                '/api/files': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                },
                '/api/code-quality': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                },
                '/api/analysis': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                },
                '/api': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                },
                '/ws': {
                    target: env.VITE_API_URL || 'http://127.0.0.1:8082',
                    changeOrigin: true,
                    secure: false,
                    ws: true,  // 启用 WebSocket 升级代理，原生 WS /ws 需要
                },
            },
        },

        build: {
            outDir: 'dist',
            sourcemap: mode === 'development',
            rollupOptions: {
                output: {
                    manualChunks: {
                        'react-vendor': ['react', 'react-dom', 'react-router-dom'],
                        'editor': ['monaco-editor'],
                        'terminal': ['@xterm/xterm', '@xterm/addon-fit'],
                        'markdown': ['react-markdown', 'react-syntax-highlighter'],
                        'ui': ['zustand', 'immer', 'react-virtuoso'],
                    },
                },
            },
            chunkSizeWarningLimit: 1000,
        },

        envPrefix: 'VITE_',

        test: {
            globals: true,
            environment: 'jsdom',
            setupFiles: './src/test-setup.ts',
            // This workspace lives on a desktop volume where many concurrent
            // transform workers can turn cold module reads into minute-long
            // stalls (and eventually ETIMEDOUT). A single fork is deterministic
            // and makes `npm run test:run` a reliable CI/local gate.
            pool: 'forks',
            fileParallelism: false,
            maxWorkers: 1,
            minWorkers: 1,
            include: [
                'src/**/*.{test,spec}.{ts,tsx}',
                'tests/**/*.{test,spec}.{ts,tsx}',
            ],
        },
    };
});
