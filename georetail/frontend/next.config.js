/** @type {import('next').NextConfig} */
const isDemo = process.env.DEMO_BUILD === "true";

const nextConfig = {
  reactStrictMode: true,
  ...(isDemo
    ? {
        output: "export",
        basePath: "/PROYECTO-FINAL",
        images: { unoptimized: true },
      }
    : {
        async rewrites() {
          return [
            {
              source: "/api/:path*",
              destination: `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/:path*`,
            },
          ];
        },
      }),
};

module.exports = nextConfig;
