import type {Config} from '@docusaurus/types';
import type {Options, ThemeConfig} from '@docusaurus/preset-classic';
import {themes as prismThemes} from 'prism-react-renderer';

const config: Config = {
  title: 'pgshard',
  tagline: 'A PostgreSQL 18 sharding platform with a Rust data plane',
  favicon: 'img/favicon.svg',
  url: 'https://andrew01234567890.github.io',
  baseUrl: '/pgshard/',
  organizationName: 'andrew01234567890',
  projectName: 'pgshard',
  trailingSlash: false,
  onBrokenLinks: 'throw',
  markdown: {
    mermaid: true,
    hooks: {onBrokenMarkdownLinks: 'throw'},
  },
  themes: ['@docusaurus/theme-mermaid'],
  plugins: [
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        indexDocs: true,
        indexBlog: false,
        indexPages: true,
        docsRouteBasePath: '/docs',
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
      },
    ],
  ],
  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: 'docs',
          showLastUpdateAuthor: false,
          showLastUpdateTime: true,
          editUrl: 'https://github.com/andrew01234567890/pgshard/edit/main/website/',
        },
        blog: false,
        theme: {customCss: './src/css/custom.css'},
        sitemap: {changefreq: 'weekly', priority: 0.5},
      } satisfies Options,
    ],
  ],
  themeConfig: {
    image: 'img/social-card.svg',
    metadata: [
      {name: 'description', content: 'Documentation for pgshard, a PostgreSQL 18 sharding platform with a Rust data plane.'},
    ],
    navbar: {
      title: 'pgshard',
      logo: {alt: 'pgshard logo', src: 'img/logo.svg'},
      items: [
        {type: 'docSidebar', sidebarId: 'docsSidebar', position: 'left', label: 'Docs'},
        {to: '/docs/quickstart', label: 'Quickstart', position: 'left'},
        {to: '/docs/operations/testing', label: 'Testing', position: 'left'},
        {href: 'https://github.com/andrew01234567890/pgshard', label: 'GitHub', position: 'right'},
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Learn',
          items: [
            {label: 'Quickstart', to: '/docs/quickstart'},
            {label: 'Architecture', to: '/docs/concepts/architecture'},
            {label: 'SQL compatibility', to: '/docs/reference/sql-compatibility'},
          ],
        },
        {
          title: 'Operate',
          items: [
            {label: 'High availability', to: '/docs/operations/high-availability'},
            {label: 'Backup and restore', to: '/docs/operations/backup-restore'},
            {label: 'Observability', to: '/docs/operations/observability'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/andrew01234567890/pgshard'},
            {label: 'Releases', to: '/docs/project/releases'},
            {label: 'Development', to: '/docs/project/development'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} pgshard contributors. Apache-2.0 licensed.`,
    },
    colorMode: {defaultMode: 'light', respectPrefersColorScheme: true},
    prism: {theme: prismThemes.github, darkTheme: prismThemes.dracula},
  } satisfies ThemeConfig,
};

export default config;
