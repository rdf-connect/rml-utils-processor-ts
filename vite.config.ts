import { defineConfig } from "vitest/config";

export default defineConfig({
    resolve: {
        tsconfigPaths: true,
    },
    test: {
        testTimeout: 20000,
        deps: {
            optimizer: {
                ssr: {
                    enabled: true,
                    include: ["@rdfc/js-runner"],
                },
            },
        },
    },
});
