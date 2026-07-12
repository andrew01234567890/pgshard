import clsx from 'clsx';
import type {JSX} from 'react';
import Heading from '@theme/Heading';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import styles from './index.module.css';

const features = [
  ['PostgreSQL-native target', 'The Milestone 1 design targets PostgreSQL 18, pgoutput, pgBackRest, and familiar PostgreSQL clients.'],
  ['Rust data-plane target', 'Planned routing, pooling, CDC, orchestration, and transaction recovery prioritize predictable latency.'],
  ['Kubernetes-native target', 'The planned Go operator will manage shards, services, scaling, backup, DDL, and reshard workflows.'],
];

export default function Home(): JSX.Element {
  return (
    <Layout title="PostgreSQL sharding with a Rust data plane" description="Documentation for pgshard.">
      <header className={clsx('hero hero--primary', styles.hero)}>
        <div className="container">
          <p className={styles.eyebrow}>MILESTONE 1 · UNDER DEVELOPMENT</p>
          <Heading as="h1" className={styles.title}>PostgreSQL sharding, built for the data path</Heading>
          <p className={styles.subtitle}>Designing and building routing, pooling, replication, backup, and resharding for PostgreSQL 18. The runtime is not available yet.</p>
          <div className={styles.actions}>
            <Link className="button button--secondary button--lg" to="/docs/project/status">Check implementation status</Link>
            <Link className="button button--outline button--secondary button--lg" to="/docs/concepts/architecture">Understand the architecture</Link>
          </div>
        </div>
      </header>
      <main>
        <section className={styles.features}>
          <div className="container">
            <div className="row">
              {features.map(([title, description]) => (
                <div className="col col--4" key={title}>
                  <div className={styles.card}>
                    <Heading as="h2">{title}</Heading>
                    <p>{description}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </section>
        <section className={styles.boundary}>
          <div className="container">
            <Heading as="h2">Correctness boundaries are part of the interface</Heading>
            <p>The Milestone 1 design limits distributed transactions to <code>READ COMMITTED</code> and targets atomic durable outcomes. No runtime guarantee is claimed before the coordinator and its fault tests are implemented.</p>
            <Link to="/docs/concepts/distributed-transactions">Read the transaction guarantees →</Link>
          </div>
        </section>
      </main>
    </Layout>
  );
}
