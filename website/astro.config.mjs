// @ts-check
import { defineConfig } from "astro/config";

import tailwindcss from "@tailwindcss/vite";

import starlight from "@astrojs/starlight";

const site = "https://ochi.dev";

// https://astro.build/config
export default defineConfig({
  site,
  vite: {
    plugins: [tailwindcss()],
  },

  integrations: [
    starlight({
      title: "Ochi Documentation",
      description: "Ochi is a logging solution",
      head: [
        {
          tag: "meta",
          attrs: { property: "og:site_name", content: "Ochi" },
        },
        {
          tag: "meta",
          attrs: { property: "og:type", content: "website" },
        },
        {
          tag: "meta",
          attrs: {
            property: "og:image",
            content: `${site}/og.png`,
          },
        },
        {
          tag: "meta",
          attrs: { property: "og:image:width", content: "1200" },
        },
        {
          tag: "meta",
          attrs: { property: "og:image:height", content: "630" },
        },
        {
          tag: "meta",
          attrs: {
            property: "og:image:alt",
            content: "Ochi Observability",
          },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:card", content: "summary_large_image" },
        },
        {
          tag: "meta",
          attrs: {
            name: "twitter:image",
            content: `${site}/og.png`,
          },
        },
      ],
      customCss: ["./src/styles/global.css", "./src/styles/theme.css"],
      favicon: "/favicon.ico",
      social: [
        {
          icon: "discord",
          label: "Discord",
          href: "https://discord.gg/AsCKpCNp5c",
        },
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/ochi-team/ochi",
        },
      ],
      logo: {
        src: "/src/assets/logo.svg",
        replacesTitle: true,
      },
      sidebar: [
        {
          label: "Guides",
          items: [{ autogenerate: { directory: "guides" } }],
        },
        {
          label: "Reference",
          items: [{ autogenerate: { directory: "reference" } }],
        },
        {
          label: "Changelog",
          items: [{ autogenerate: { directory: "changelog" } }],
        },
      ],
      defaultLocale: "en",
      locales: {
        root: {
          label: "English",
          lang: "en",
        },
      },
    }),
  ],
});
