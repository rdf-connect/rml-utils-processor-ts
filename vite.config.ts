import { defineConfig } from "vitest/config";

export default defineConfig({
    resolve: {
        tsconfigPaths: true,
    },
    test: {
        testTimeout: 60000,
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
