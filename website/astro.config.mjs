// @ts-check
import { defineConfig } from 'astro/config';

import tailwindcss from '@tailwindcss/vite';

import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
  vite: {
    plugins: [tailwindcss()]
  },

  integrations: [
    starlight({
      title: 'Ochi Documentation',
      customCss: ['./src/styles/theme.css'],
      favicon: '/favicon.ico',
      social: [
        { icon: 'discord', label: 'Discord', href: 'https://discord.gg/AsCKpCNp5c' },
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/ochi-team/ochi',
        },
      ],
      logo: {
        src: '/src/assets/logo.svg',
        replacesTitle: true,
      },
      sidebar: [
        {
          label: 'Guides',
          items: [
            { label: 'Installation', slug: 'docs/guides/installation' },
          ],
        },
        {
          label: 'Changelog',
          items: [
            { label: '0.1', slug: 'docs/changelog/0.1' },
          ],
        },
      ],
      defaultLocale: 'en',
      locales: {
        root: {
          label: 'English',
          lang: 'en',
        },
      },
    }),
  ],
});
