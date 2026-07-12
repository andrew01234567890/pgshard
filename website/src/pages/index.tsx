import clsx from 'clsx';
import type {JSX} from 'react';
import Heading from '@theme/Heading';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import styles from './index.module.css';

const features = [
  ['PostgreSQL-native', 'PostgreSQL 18, pgoutput logical replication, pgBackRest, and familiar PostgreSQL clients.'],
  ['Rust data plane', 'Routing, pooling, CDC, orchestration, and distributed transaction recovery prioritize predictable latency.'],
  ['Operated on Kubernetes', 'A Go operator manages shards, HA, services, scaling, backup, DDL, and reshard workflows.'],
];

export default function Home(): JSX.Element {
  return (
    <Layout title="PostgreSQL sharding with a Rust data plane" description="Documentation for pgshard.">
      <header className={clsx('hero hero--primary', styles.hero)}>
        <div className="container">
          <p className={styles.eyebrow}>MILESTONE 1 · ALPHA</p>
          <Heading as="h1" className={styles.title}>PostgreSQL sharding, built for the data path</Heading>
          <p className={styles.subtitle}>Route, pool, replicate, back up, and reshard PostgreSQL 18 clusters with a Rust data plane and Kubernetes-native control plane.</p>
          <div className={styles.actions}>
            <Link className="button button--secondary button--lg" to="/docs/quickstart">Start with the quickstart</Link>
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
            <p>Distributed writes are atomic and durable through 2PC at <code>READ COMMITTED</code>. Milestone 1 does not claim global snapshots, serializability, or simultaneous visibility across shards.</p>
            <Link to="/docs/concepts/distributed-transactions">Read the transaction guarantees →</Link>
          </div>
        </section>
      </main>
    </Layout>
  );
}
