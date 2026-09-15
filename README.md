# CNC Lakehouse

Plateforme data **temps réel** pour un atelier de 10 machines CNC simulées :
ingestion en streaming, lakehouse en couches (Bronze / Silver / Gold), qualité des données,
orchestration et tableaux de bord.

> Projet personnel de Data Engineering, construit étape par étape sur une machine locale (16 Go de RAM).

## Architecture cible

```mermaid
flowchart LR
    SIM[Simulateur Python<br/>10 machines CNC] -->|JSON| K[(Kafka)]
    K -->|Spark Structured Streaming| B[Bronze<br/>Iceberg]
    B -->|nettoyage, dédoublonnage| S[Silver<br/>Iceberg]
    S -->|agrégats métier| G[Gold<br/>KPIs, OEE, pannes]
    G --> T[Trino SQL]
    T --> D[Dashboard]
    AF[Airflow] -.orchestre.-> B & S & G
```

## Avancement

- [x] **Étape 1** : Kafka + simulateur de machines (données volontairement imparfaites)
- [ ] **Étape 2** : stockage objet MinIO + tables Iceberg + Spark Streaming vers Bronze
- [ ] **Étape 3** : couche Silver (dédoublonnage, retards, valeurs aberrantes)
- [ ] **Étape 4** : couche Gold (disponibilité, OEE, MTBF/MTTR) + Trino
- [ ] **Étape 5** : orchestration Airflow
- [ ] **Étape 6** : tests de qualité des données + CI GitHub Actions
- [ ] **Étape 7** : dashboard + détection d'anomalies (maintenance prédictive)

## Données simulées

| Topic Kafka | Contenu | Clé |
|---|---|---|
| `cnc.telemetry` | 1 mesure par machine par seconde : vitesse de broche, température, vibrations, puissance, usure de l'outil | `machine_id` |
| `cnc.machine-events` | changements d'état : panne (avec code), maintenance corrective ou préventive, pause, reprise | `machine_id` |

Environ 2 % des mesures sont volontairement défectueuses (doublons, retards de 5 à 30 min,
valeurs nulles, valeurs impossibles) pour être traitées dans la couche Silver.

## Démarrage rapide

Prérequis : Docker Desktop, [uv](https://docs.astral.sh/uv/).

```bash
docker compose up -d          # Kafka + Kafka UI + création des topics
uv sync                       # dépendances Python
uv run pytest                 # tests du simulateur
uv run python -m simulator.producer
```

Kafka UI : http://localhost:8085
