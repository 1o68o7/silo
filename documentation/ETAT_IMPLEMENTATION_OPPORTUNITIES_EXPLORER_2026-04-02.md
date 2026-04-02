# État d’implémentation — Opportunités, Explorer, suivi maillage (Silo Lab)

**Date :** 2026-04-02  
**Brief API à jour :** [`BRIEF_REPONSE_OPPORTUNITIES_EXPLORER_2026-04-02.md`](./BRIEF_REPONSE_OPPORTUNITIES_EXPLORER_2026-04-02.md).  
**Runbook :** [`SILO_MIGRATION_OPPORTUNITY_RECORDS_RUNBOOK.md`](./SILO_MIGRATION_OPPORTUNITY_RECORDS_RUNBOOK.md).

---

## 1. Actions frontend (`log8ot-frontend`)

| Zone | Détail |
|------|--------|
| **Client API** (`lib/silo-api.ts`) | Types `OpportunityRecord` + `implemented*` ; `SiloGraphEdge.edge_kind` ; **`patchOpportunityRecord`** ; **`listOpportunityRecords`** / **`getOpportunitiesProjectSummary`** doivent envoyer le **JWT** sur GET si `SILO_REQUIRE_AUTH=true`. |
| **Graphe** (`.../explorer/graph-canvas.tsx`) | `opportunityEdges` ; style distinct opportunité vs structure. |
| **Explorer** (`.../explorer/page.tsx`) | Records, filtres URLs, option « réalisés », onglets arborescence / liens crawl. |
| **Suivi maillage** | `/dashboard/silo-lab/suivi-maillage` — case « Fait » → `patchOpportunityRecord`. |
| **Navigation** | Entrée Suivi maillage dans la sidebar. |

**À valider côté front :** Bearer token sur **`GET .../opportunities/records`** et **`GET .../opportunities/summary`** (sinon **401** en prod).

---

## 2. Backend Silo (ce dépôt) — statut **livré**

| Priorité | Action | Statut |
|----------|--------|--------|
| P0 | Migration `opportunity_records` (`implemented*`) | **Fait** — `scripts/migrate_opportunity_records_implemented.sql` |
| P0 | `PATCH .../opportunities/records/{recordId}` | **Fait** |
| P0 | Sérialisation `GET .../records` avec `implemented*` | **Fait** |
| P1 | Auth `GET .../records` + `GET .../summary` | **Fait** (`require_auth`) |
| P1 | Upsert `save` + contrainte unique `(project, source, cible)` | **Fait** |
| P2 | `GET .../opportunities/summary` | **Fait** |
| P2 | `edge_kind: "crawl"` sur `GET .../graph` | **Fait** |
| P2 | Query `page_ids`, `limit`, `offset` sur records | **Fait** |

**Décisions produit** (2026-04-02) : **toggle Backlog / Suggestions** (C) ; crawl : masquer arête opportunité si arête crawl **même sens**, badge Suivi — voir `BRIEF_REPONSE_...` § Décisions produit. **Reste** : implémentation front.

---

## 3. Upload vers l’API Docs (recommandé)

Inclure ce fichier + le brief + le runbook dans le script `upload-docs-to-api.sh` du repo front, selon `Cursor_INIT_DEV_STACK.md`.

---

*Mise à jour 2026-04-02 — alignement post-déploiement backend.*
