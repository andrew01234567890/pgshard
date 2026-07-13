import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'overview',
    'quickstart',
    {
      type: 'category',
      label: 'Concepts',
      collapsed: false,
      items: [
        'concepts/architecture',
        'concepts/shardschema',
        'concepts/distributed-transactions',
        'concepts/change-streams',
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      collapsed: false,
      items: [
        'operations/high-availability',
        'operations/backup-restore',
        'operations/online-ddl',
        'operations/online-resharding',
        'operations/observability',
        'operations/testing',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: ['reference/sql-compatibility'],
    },
    {
      type: 'category',
      label: 'Project',
      items: ['project/releases', 'project/development'],
    },
  ],
};

export default sidebars;
