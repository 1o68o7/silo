# Brief de réponse backend — Opportunités, maillage réalisé, Explorer

**Contexte :** Silo Lab Explorer, suivi des liens, distinction crawl vs suggestions.  
**Périmètre :** ce dépôt (`main.py`, `database/models.py`, `database/service.py`).

**Historique :** première analyse 2026-04-02. **Mise à jour :** même date — implémentation backend (migration `scripts/migrate_opportunity_records_implemented.sql`, API `PATCH` / `summary`, auth alignée, upsert `save`, `edge_kind` sur le graphe).  
**Runbook exploitation :** [`SILO_MIGRATION_OPPORTUNITY_RECORDS_RUNBOOK.md`](./SILO_MIGRATION_OPPORTUNITY_RECORDS_RUNBOOK.md).

---

## Synthèse

Le backend distingue toujours **trois couches** :

1. **Graphe crawl** — table `edges` ; `GET /api/projects/{id}/graph` inclut désormais **`edge_kind: "crawl"`** sur chaque arête (les suggestions opportunité restent fusionnées côté client à partir des records ou autres flux).
2. **Suggestions pré-calculées** — `computed_opportunities`, après `POST /api/opportunities/{id}/compute`.
3. **Fiches enregistrées** — `opportunity_records`, via `POST /api/projects/{id}/opportunities/save` (**upsert** sur `project_id` + `source_page_id` + `target_page_id`).

Le **statut « maillage réalisé »** est porté par **`OpportunityRecord`** (`implemented`, `implemented_at`, `implemented_by`, `implementation_note`) et mis à jour via **`PATCH .../opportunities/records/{recordId}`**.  
Un agrégat dédié évite les heuristiques client : **`GET /api/projects/{id}/opportunities/summary`**.

---

## 1. Persistance du statut « maillage réalisé »

### Implémenté (modèle + SQL)

Sur `OpportunityRecord` :

| Colonne | Type | Rôle |
|---------|------|------|
| `implemented` | bool (défaut `false`) | Lien considéré comme réalisé sur le site |
| `implemented_at` | `timestamp` nullable | Renseigné lors du passage à `implemented=true` |
| `implemented_by` | `varchar(255)` nullable | `user_id` JWT si auth active et non anonyme |
| `implementation_note` | `text` nullable | Note libre |

### Identité stable

- Unicité métier : **`(project_id, source_page_id, target_page_id)`** — index unique en base après migration ; **upsert** au `save`.
- Clé API : **`OpportunityRecord.id`** pour `PATCH` et `DELETE`.

`ComputedOpportunity` reste recalculé par le worker ; le suivi « fait » est sur **`OpportunityRecord`** uniquement.

---

## 2. Contrat `GET /api/projects/{projectId}/opportunities/records`

### Auth

**`require_auth`** aligné sur `save` / `delete` / `PATCH` lorsque `SILO_REQUIRE_AUTH=true` — envoyer **`Authorization: Bearer <JWT>`** (identité Silo : claim `user_id`).

### Réponse

`{ "records": [ ... ] }`.

### Query

| Paramètre | Description |
|-----------|-------------|
| `page_id` | Filtre : page **source ou cible** (comportement historique). |
| `page_ids` | Répéter : `?page_ids=a&page_ids=b` — enregistrements touchant **au moins une** de ces pages. Si `page_ids` est fourni (non vide), il **prime** sur `page_id` côté service. |
| `limit` | Taille max (plafonné à **10000**) ; **omis ou `≤0`** = pas de limite côté requête. |
| `offset` | Décalage (≥ 0). |

### Exemple d’enregistrement (sérialisation actuelle)

```json
{
  "id": 42,
  "source": "pageIdSource",
  "target": "pageIdCible",
  "source_url": "https://example.com/a",
  "target_url": "https://example.com/b",
  "similarity": 0.91,
  "zone_texte": null,
  "phrase_ancre_proposee": null,
  "created_at": "2026-04-02T12:00:00.000000Z",
  "implemented": false,
  "implemented_at": null,
  "implemented_by": null,
  "implementation_note": null
}
```

---

## 3. `GET /api/projects/{projectId}/opportunities/summary`

**Auth :** identique à `GET .../records`.

Réponse indicative (chaînes ISO en `*_at` avec suffixe `Z` quand présent) :

| Champ | Signification |
|--------|-----------------|
| `project_id` | Id projet |
| `saved_count` | Nombre de `opportunity_records` |
| `last_saved_at` | `max(created_at)` sur les records |
| `has_saved_opportunities` | `saved_count > 0` |
| `computed_count` | Lignes `computed_opportunities` (global) |
| `computed_at` | Dernier calcul batch |
| `has_computed_opportunities` | `computed_count > 0` |
| `has_embeddings` | Au moins une page avec embedding |
| `embeddings_status` | Objet détaillé (éligibles, progression, etc.) |

**Décision produit — validée** : voir [§ Décisions produit](#décisions-produit-validées-2026-04-02) (option **C** + règles crawl).

---

## 4. Graphe `GET /api/projects/{id}/graph`

Chaque arête issue du crawl inclut :

- `source`, `target`, `weight`, `anchor`
- **`edge_kind: "crawl"`**

Les arêtes « opportunité » (suggestions) restent **dynamiques côté client** (records / autres endpoints). Règle « déjà dans le crawl » : **côté front** — voir [§ Décisions produit](#décisions-produit-validées-2026-04-02).

**Convention visuelle (Explorer)** : **trait plein** = liens de **structure** (crawl) ; **trait pointillé** = **suivi de maillage** (opportunités affichées). Détail §3 dans « Décisions produit ».

---

## 5. `PATCH /api/projects/{projectId}/opportunities/records/{recordId}`

### Corps

```json
{
  "implemented": true,
  "implementation_note": "optionnel"
}
```

- **`implemented`** : bool, **obligatoire**.
- **`implementation_note`** : optionnel ; si la clé est **présente** dans le JSON, la note est mise à jour (chaîne vide pour effacer). Si la clé est **absente**, la note inchangée.

### Comportement

- **Idempotence** : `implemented_at` / `implemented_by` ne sont mis à jour que lorsque la valeur **`implemented` change**.
- **Erreurs** : `401` (auth), `404` (projet ou record), `422` (corps invalide), `500` (erreur serveur).

### Réponse

```json
{
  "ok": true,
  "record": { "...": "même forme qu’un élément de GET .../records" }
}
```

---

## 6. `POST /api/projects/{projectId}/opportunities/save`

**Upsert** sur la triplette projet / source / cible : met à jour `similarity`, `zone_texte`, `phrase_ancre_proposee` si la paire existe **sans** réinitialiser `implemented*` ni `created_at`.

---

## 7. Cohérence multi-URL / performance

- Filtres **`page_id`** et **`page_ids`**, pagination **`limit` / `offset`** sur `GET .../records`.
- Volumétrie très grande : envisager côté front pagination incrémentale avec `limit` / `offset`.

---

## Décisions produit (validées 2026-04-02)

### 1. Affichage conditionnel Explorer — **option C** (deux modes)

Un **toggle** distinct (libellés suggérés : **« Backlog »** / **« Suggestions »**) :

| Mode | Source de données | Usage |
|------|-------------------|--------|
| **Backlog** | `opportunity_records` (`GET .../opportunities/records`) | Paires **enregistrées** par l’utilisateur ; filtres graphe, suivi **Fait** (`PATCH`) alignés là-dessus. |
| **Suggestions** | Opportunités **computed** (`GET /api/opportunities/{id}` après calcul worker) | Exploration **sans** passage obligatoire par « save » ; volumétrie potentiellement **élevée** ; pas de champ `implemented` tant que la paire n’est pas un record. |

**Aide API** : `GET .../opportunities/summary` expose `has_saved_opportunities` et `has_computed_opportunities` pour activer les modes, afficher un état vide, ou pré-sélectionner un onglet.

### 2. Lien opportunité **déjà présent** dans le crawl (`edges`)

Règles **figées pour l’implémentation front** (sens strict = cohérent avec le sens du maillage proposé) :

1. **Correspondance** : considérer que le crawl **couvre** la suggestion si une arête du graphe (crawl, `edge_kind: "crawl"`) existe avec le **même couple** **`source` → `target`** que la paire opportunité (pas d’équivalence inverse ; évite les faux « déjà fait » quand seul B→A existe).
2. **Graphe** : **ne pas afficher** l’arête « opportunité » pour cette paire (évite doublon visuel avec le lien crawl).
3. **Suivi maillage** : **badge ou colonne** du type « Déjà dans le crawl » lorsque la condition (1) est vraie.
4. **Hors périmètre immédiat** : pas de passage automatique à `implemented: true` ; une suggestion UX « Marquer comme fait » peut venir en **phase 2**.

*Si vous souhaitez plus tard comparer les liens **dans un sens ou l’autre** (symétrique), documenter l’écart ici et ajuster la condition (1).*

### 3. Rendu des arêtes sur le graphe (structure vs maillage)

| Style | Signification | Source typique |
|-------|---------------|----------------|
| **Trait plein** | Liens de **structure** réellement observés au crawl | Arêtes `GET .../graph` (`edge_kind: "crawl"`) ; même convention si le front trace les liens d’**arborescence** URL sur le même canvas. |
| **Trait pointillé** | **Suivi de maillage** — opportunités (backlog enregistré ou suggestions selon le mode) | Arêtes dérivées des records / paires computed, **hors** doublon avec un lien crawl déjà présent (même sens) — voir §2 point 2. |

Cette convention est **validée produit** ; la légende de l’Explorer doit l’expliciter (ex. *Structure* / *Opportunité* ou *Maillage*).

---

## Livrables — bilan (post-implémentation)

| Livrable | Statut |
|----------|--------|
| Schéma `implemented*` + migration | **Fait** |
| `GET summary` | **Fait** |
| `GET records` enrichi + auth + filtres | **Fait** |
| `PATCH` statut réalisé | **Fait** |
| `edge_kind: "crawl"` sur graphe | **Fait** |
| Upsert `save` | **Fait** |
| Décisions produit §Explorer + crawl vs opportunité | **Validées** — voir section « Décisions produit » ; implé **front** à suivre |

---

## Références code

- Modèle : `database/models.py` — `OpportunityRecord`
- Service : `database/service.py` — `_serialize_opportunity_record`, `save_opportunity_records`, `list_opportunity_records`, `patch_opportunity_record_implementation`, `get_opportunities_project_summary`, `get_graph`
- Routes : `main.py` — préfixe `/api/projects/{project_id}/opportunities/...`

---

## Liste des routes utiles (rappel)

| Méthode | Route |
|---------|--------|
| GET | `/api/opportunities/{projectId}` |
| POST | `/api/opportunities/{projectId}/compute` |
| POST | `/api/projects/{projectId}/opportunities/save` |
| GET | `/api/projects/{projectId}/opportunities/records` |
| GET | `/api/projects/{projectId}/opportunities/summary` |
| PATCH | `/api/projects/{projectId}/opportunities/records/{recordId}` |
| DELETE | `/api/projects/{projectId}/opportunities/records/{recordId}` |
| GET | `/api/projects/{projectId}/pages/{pageId}/opportunities` |
| GET | `/api/projects/{projectId}/graph` |
