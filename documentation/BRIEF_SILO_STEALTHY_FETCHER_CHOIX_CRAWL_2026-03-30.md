# Brief — Choix `SILO_USE_STEALTHY_FETCHER` au lancement du crawl

**Date :** 2026-03-30  
**Périmètre :** Worker Silo (fetch HTML), API, admin, orchestration (Docker / Kubernetes), **Front** (formulaire crawl, états, erreurs).  
**État :** spécification produit / technique (non implémenté).

---

## 1. Contexte

Aujourd’hui, le mode de récupération des pages est fixé au **démarrage du processus worker** via la variable d’environnement `SILO_USE_STEALTHY_FETCHER` (défaut `true`). La valeur est lue **à l’import** du module `worker/fetcher.py` ; un changement sans redémarrage du processus **n’a aucun effet** sur les crawls en cours ou futurs tant que le worker n’est pas relancé.

Deux modes coexistent :

| Valeur | Comportement | Effet secondaire notable |
|--------|----------------|---------------------------|
| `true` | Scrapling `StealthyFetcher` (navigateur headless, JS exécuté) | Meilleure tolérance anti-bot ; scripts tiers (ex. GTM/GA4) peuvent être chargés comme pour un navigateur réel. |
| `false` | Trafilatura / aiohttp (HTTP, pas d’exécution JS) | Plus léger et rapide sur sites sans protection ; pas d’exécution des balises analytics dans le fetch. |

Les exploitants ou l’utilisateur final ont besoin de **choisir explicitement** ce mode **au moment où ils lancent un crawl**, et de **garantir** que le moteur worker tourne avec la bonne configuration **avant** d’enfiler le job.

---

## 2. Objectif

Permettre de **sélectionner** `SILO_USE_STEALTHY_FETCHER` à `true` ou `false` **dans le flux « lancer un crawl »**, et **aligner le worker** sur ce choix en **redémarrant le moteur** si la configuration persistée ne correspond pas au choix, puis **enchaîner** avec le déclenchement du crawl (même session utilisateur / même API).

---

## 3. Parcours cible (produit)

1. L’utilisateur ouvre le flux de lancement de crawl (formulaire ou endpoint dédié).
2. Il voit une option du type : **« Fetch navigateur (anti-bot, JS actif) »** vs **« Fetch HTTP simple (plus rapide, sans JS) »**, mappée en interne sur `SILO_USE_STEALTHY_FETCHER=true` / `false`.
3. Au clic sur « Lancer » :
   - le backend **lit** la configuration effective actuelle du worker (voir §6) ;
   - si elle **diffère** du choix : **redémarrage orchestré** du service worker (drain / stop / start avec la nouvelle env) ;
   - une fois le worker **prêt** (healthcheck OK) : **envoi du job de crawl** en queue (comportement actuel).

Messages utilisateur recommandés :

- Pendant redémarrage : *« Application du mode de crawl… redémarrage du moteur (quelques secondes). »*
- En cas d’échec du redémarrage : erreur explicite, **pas** de crawl lancé avec un mode incertain.

---

## 4. Exigences fonctionnelles

- **EF1** — Le choix utilisateur est **persisté** de façon fiable pour le worker (au minimum : variable d’environnement du conteneur / unit systemd / process manager au redémarrage).
- **EF2** — Avant tout nouveau lancement de crawl, le système **vérifie** que le worker actif utilise la même valeur que le choix (ou redémarre puis revérifie).
- **EF3** — Un crawl **ne doit pas** démarrer si, après N tentatives / timeout, le worker n’expose pas le mode attendu (échec contrôlé).
- **EF4** — Journaliser dans les logs de crawl (ou logs applicatifs) le mode effectif : `use_stealthy_fetcher: true|false` pour audit et support.
- **EF5** — Le **Front** affiche le **mode actuellement actif** côté worker (ou « inconnu » si indisponible) pour éviter toute ambiguïté avant le lancement.

---

## 5. API — spécification (Back)

> Alignement sur l’existant : `CrawlConfig` (`main.py`), `POST /api/projects/{project_id}/crawl`, `GET /api/config` (`CrawlConfigResponse`).

### 5.1 Extension du modèle `CrawlConfig` (body `POST .../crawl`)

Ajouter un champ **optionnel** :

| Champ | Type | Défaut | Description |
|-------|------|--------|-------------|
| `use_stealthy_fetcher` | `boolean` **ou** `null` | `null` | `true` = navigateur headless ; `false` = HTTP simple ; `null` = **ne pas exiger** de changement : utiliser le mode déjà en cours sur le worker (comportement rétrocompatible). |

**Règles :**

- Si `use_stealthy_fetcher` est `true` ou `false` : le backend **doit** garantir ce mode **avant** `RPUSH` sur `silo:crawl_queue` (redémarrage worker si nécessaire, puis attente *ready* + vérification).
- Si `null` / absent : **aucun** redémarrage pour aligner le mode ; enqueue immédiat comme aujourd’hui (le worker utilise sa config au démarrage).

### 5.2 Extension de `GET /api/config`

Étendre la réponse (ex. nouveau modèle ou champs ajoutés à `CrawlConfigResponse`) pour le **formulaire Front** :

| Champ proposé | Type | Description |
|---------------|------|-------------|
| `fetch_mode_options` | `array` | Liste d’objets pour construire le sélecteur UI, ex. `[{ "value": true, "label_key": "crawl.fetch.stealthy", "description_key": "crawl.fetch.stealthy.help" }, ...]`. Les clés i18n permettent au Front de rester générique. |
| `default_use_stealthy_fetcher` | `boolean` | Valeur par défaut du formulaire (souvent alignée sur `SILO_USE_STEALTHY_FETCHER` du **déploiement** API ou d’une config centralisée). |
| `worker_runtime_use_stealthy_fetcher` | `boolean \| null` | Valeur **réellement** annoncée par le worker au runtime (Redis / health). `null` = worker injoignable ou non instrumenté. |
| `worker_healthy` | `boolean` | Indique si le dernier healthcheck worker est OK. |
| `worker_restart_supported` | `boolean` | Si `false`, le Front peut masquer l’option « forcer le mode » ou afficher un avertissement (*« contactez l’administrateur »*). Utile quand l’API n’a pas les droits d’orchestration. |

Les champs existants `path_prefix_options` et `default_exclude_urls_with_params` restent inchangés.

### 5.3 Endpoint admin / runtime (recommandé)

Pour éviter de surcharger `GET /api/config` avec des appels lourds, ou pour réserver l’accès aux rôles admin :

| Méthode | Route (proposition) | Rôle | Description |
|---------|---------------------|------|-------------|
| `GET` | `/api/admin/worker/fetch-config` | Admin (ou même auth que le crawl si politique unique) | Retourne : `desired_use_stealthy` (config persistée), `runtime_use_stealthy`, `healthy`, `last_restart_at`, `restart_in_progress`. |
| `POST` | `/api/admin/worker/restart` | Admin | **Optionnel** : redémarrage explicite sans crawl ; body `{ "use_stealthy_fetcher": boolean }`. Utile pour le support. |

*Si pas de rôle admin distinct* : exposer uniquement les champs nécessaires sur `GET /api/config` avec le même `require_auth` que le reste de l’app.

### 5.4 Comportement de `POST /api/projects/{project_id}/crawl`

**Séquence côté API (pseudo-flow) :**

1. Valider le projet (existant aujourd’hui).
2. Lire `use_stealthy_fetcher` depuis le body.
3. Si booléen explicite :
   - comparer avec `runtime` worker ;
   - si différent : **orchestrer restart** (§8) puis boucle **poll** *healthy* + cohérence `runtime` (timeout configurable, ex. `SILO_WORKER_ALIGN_TIMEOUT_SEC=120`) ;
   - si échec : **ne pas** enqueue ; retourner erreur (§5.5).
4. Si OK ou champ absent/`null` : construire le payload Redis comme aujourd’hui (`project_id`, `seed_url`, `max_depth`, …) et `RPUSH`.

**Optionnel** : inclure dans le payload Redis un champ `requested_fetch_mode` uniquement pour **logs** (le worker ne lit pas forcément ce champ au MVP).

### 5.5 Codes HTTP et corps d’erreur (proposition)

| Code | Cas | Corps (exemple JSON) |
|------|-----|----------------------|
| `200` | Crawl enfilé (aucun restart ou restart terminé dans la fenêtre synchrone) | `{ "ok": true, "message": "...", "worker_action": "none" \| "restarted", "use_stealthy_fetcher": true }` |
| `202` | Redémarrage accepté, crawl **pas encore** enfilé (si mode async choisi) | `{ "status": "aligning_worker", "poll_url": "/api/projects/{id}/crawl-alignment/{op_id}" }` |
| `409` | Conflit : autre alignement en cours, ou crawl incompatible | `{ "detail": "...", "code": "WORKER_ALIGN_IN_PROGRESS" }` |
| `503` | Worker ou Redis indisponible | `{ "detail": "...", "code": "WORKER_UNAVAILABLE" }` |
| `504` | Timeout alignement / redémarrage | `{ "detail": "...", "code": "WORKER_ALIGN_TIMEOUT" }` |

**Décision produit** : privilégier **une réponse synchrone** jusqu’à enqueue (`200`) pour simplifier le Front **tant que** le timeout HTTP côté client (proxy/load balancer) est supérieur au timeout alignement (ex. client 180 s, API 120 s). Sinon, mode `202` + poll (§9 Front).

### 5.6 Variables d’environnement (API / worker)

| Variable | Exemple | Rôle |
|----------|---------|------|
| `SILO_USE_STEALTHY_FETCHER` | `true` / `false` | Valeur au **démarrage** du worker (inchangé). |
| `SILO_WORKER_ALIGN_TIMEOUT_SEC` | `120` | Timeout côté API pour attendre le worker après restart. |
| `SILO_WORKER_RESTART_LOCK_KEY` | défaut Redis | Clé de verrou pour éviter deux restarts concurrents. |
| `SILO_ORCHESTRATION_*` | selon impl. | Credentials / URL pour Docker socket, K8s API, etc. (§8). |

---

## 6. Source de vérité runtime (worker)

- **À l’initialisation du worker** : après lecture de `SILO_USE_STEALTHY_FETCHER`, écrire dans Redis (ex.)  
  `SET silo:worker_runtime:use_stealthy_fetcher 1|0`  
  et éventuellement `SET silo:worker_runtime:heartbeat` avec TTL.refresh périodique.
- L’API compare cette valeur au choix du crawl **avant** enqueue.
- Le **healthcheck** HTTP du worker (si ajouté) peut exposer le même booléen en JSON pour double vérification.

---

## 7. Admin — périmètre, sécurité, observabilité

### 7.1 Qui peut déclencher un redémarrage ?

- **Minimum** : même utilisateur authentifié que celui qui lance le crawl, **si** l’API est autorisée à orchestrer l’infra (risque opérationnel à assumer).
- **Recommandé** : rôle `admin` ou `silo.operator` pour `POST .../admin/worker/restart` ; les utilisateurs standard envoient seulement le booléen dans le crawl et l’API redémarre **dans les limites** de la politique org (feature flag `SILO_ALLOW_USER_TRIGGERED_WORKER_RESTART=true`).

### 7.2 Audit

- Logger : `user_id`, `project_id`, `use_stealthy_fetcher` demandé, `restarted: bool`, `duration_ms`, `outcome`.
- Prévoir corrélation avec les logs conteneur (request ID).

### 7.3 Runbook

- Documenter : durée typique de restart, impact sur crawls en cours, procédure manuelle si l’API échoue (`docker compose restart` / `kubectl rollout restart`).

---

## 8. Orchestration Docker et Kubernetes

### 8.1 Docker Compose (développement / petites prod)

**Principe :** le service `worker` lit `SILO_USE_STEALTHY_FETCHER` depuis `environment` ou `env_file`. Pour **changer** le mode sans éditer le fichier à la main :

1. **Option A** — Script ou API qui exécute sur l’hôte (dangereux si socket non sécurisé) :  
   `docker compose up -d --no-deps worker` après mise à jour de `.env` ou `export SILO_USE_STEALTHY_FETCHER=...`
2. **Option B** — Deux profils Compose : `worker-stealthy` et `worker-light` ; **scale** l’un à 0 et l’autre à 1 (avance vers le modèle « deux workers »).
3. **Option C** — Image unique ; variable passée via `environment:` ; commande `docker compose run` / **recreate** :  
   `SILO_USE_STEALTHY_FETCHER=false docker compose up -d --force-recreate worker`

**Healthcheck :** ajouter dans le `Dockerfile.worker` / `docker-compose.yml` un `HEALTHCHECK` qui appelle un petit endpoint ou vérifie que le process worker répond (ou script `redis-cli GET silo:worker_runtime:heartbeat`).

**Brief pour l’équipe infra :** l’API Silo doit pouvoir invoquer **une commande documentée** (wrapper shell, GitHub Action, webhook) avec le nouveau booléen ; ne pas exposer le socket Docker à Internet sans TLS/contrôle d’accès.

### 8.2 Kubernetes

**Principe :** le `Deployment` du worker utilise `env` depuis une `ConfigMap` / `Secret` (ex. clé `SILO_USE_STEALTHY_FETCHER`).

- **Changement de mode** :  
  1. `kubectl patch configmap silo-worker-config --patch '{"data":{"SILO_USE_STEALTHY_FETCHER":"false"}}'`  
  2. `kubectl rollout restart deployment/silo-worker`  
  3. `kubectl rollout status deployment/silo-worker`
- L’API peut utiliser le **client Go/Python Kubernetes** avec un `ServiceAccount` limité au namespace (`deployments/rollout`, `get pods`, `get deployment`).
- **Probes :** `readinessProbe` sur endpoint HTTP worker ou exec qui lit Redis ; ne marquer *ready* qu’après écriture de `silo:worker_runtime:use_stealthy_fetcher`.
- **Alternative sans patch ConfigMap :** deux `Deployment` (`silo-worker-stealthy`, `silo-worker-http`) et une seule file Redis avec **routing** — hors scope du MVP « restart dynamique » mais à mentionner en roadmap.

### 8.3 Synthèse infra

| Plateforme | Action typique | Prérequis API |
|------------|----------------|----------------|
| Compose | `up -d --force-recreate worker` + env | Accès SSH/agent ou runner interne |
| Kubernetes | `rollout restart` + ConfigMap patch | RBAC `ServiceAccount` |

---

## 9. Front — implémentation (équipe UI)

### 9.1 Données à consommer

- **`GET /api/config`** (ou équivalent enrichi §5.2) : libellés, défauts, `worker_runtime_use_stealthy_fetcher`, `worker_healthy`, `worker_restart_supported`.
- **`POST /api/projects/{id}/crawl`** : body existant + `use_stealthy_fetcher` (booléen ou omis pour rétrocompat).

### 9.2 Composants UX recommandés

- **Contrôle** : deux **radio cards** ou un **switch** avec libellés clairs (éviter le jargon « Stealthy » seul en prod).
  - Titre A : *« Navigateur (recommandé si le site bloque les robots) »* → `true`
  - Titre B : *« Récupération HTTP rapide (sans exécution JavaScript) »* → `false`
- **Texte d’aide court** sous chaque option (SEO / analytics côté tiers uniquement en mode navigateur — formulation conforme au produit).
- **Badge** : *« Moteur actuel : … »* synchronisé avec `worker_runtime_use_stealthy_fetcher` ; si `null` ou `!worker_healthy`, afficher un **avertissement** et désactiver le bouton Lancer ou proposer *« Réessayer »*.

### 9.3 États et feedback pendant le lancement

1. **Idle** : formulaire éditable.
2. **Submitting** : désactiver le bouton ; message *« Lancement du crawl… »* ; si l’API indique un redémarrage long, sous-message §3.
3. **Mode async (`202`)** si retenu côté Back : afficher une **progress bar indéterminée** ; poll `poll_url` toutes les 2–3 s ; max time alignée sur timeout serveur + marge.
4. **Success** : toast + redirection vers la vue crawl / logs existante (`GET .../crawl-status`, `.../crawl-logs`).
5. **Error** : mapper `code` (`WORKER_ALIGN_TIMEOUT`, etc.) vers messages utilisateur ; bouton *Réessayer*.

### 9.4 Types TypeScript (exemple indicatif)

```typescript
type CrawlConfigPayload = {
  max_depth?: number;
  max_pages?: number;
  run_ner?: boolean;
  seed_url?: string | null;
  url_list?: string[] | null;
  path_prefix?: string | null;
  exclude_urls_with_params?: boolean;
  /** Si omis : pas de contrainte de mode, pas de redémarrage pour alignement */
  use_stealthy_fetcher?: boolean | null;
};

type SiloApiConfig = {
  path_prefix_options: string[];
  default_exclude_urls_with_params: boolean;
  fetch_mode_options?: Array<{
    value: boolean;
    label_key: string;
    description_key: string;
  }>;
  default_use_stealthy_fetcher?: boolean;
  worker_runtime_use_stealthy_fetcher?: boolean | null;
  worker_healthy?: boolean;
  worker_restart_supported?: boolean;
};
```

### 9.5 Accessibilité et i18n

- Associer `label` + `description` aux radios (`aria-describedby`).
- Toutes les chaînes utilisateur via clés i18n ; pas de texte « SILO_USE_STEALTHY » en interface.
- Contraste des badges d’état (healthy / unhealthy).

### 9.6 Tests Front suggérés

- Mock `GET /api/config` avec `worker_restart_supported: false` → pas de promesse de changement de mode, ou UI en lecture seule.
- Mock `POST` succès avec `worker_action: "restarted"` → vérifier affichage du message intermédiaire si exposé dans le body.
- Mock erreur `504` → message timeout + réessai.

### 9.7 Cohérence avec le client HTTP existant

- Vérifier **timeout** du client (axios/fetch) : si le Back répond en synchrone jusqu’à 120 s, augmenter le timeout **uniquement** pour cet endpoint ou utiliser le flux `202` + poll.
- Ne pas casser les appels existants : champ `use_stealthy_fetcher` **optional**.

---

## 10. Risques et décisions produit

- **Crawl concurrent** : si deux utilisateurs lancent en même temps avec des modes opposés, le dernier gagne après restart — traiter par **verrou Redis** `SILO_WORKER_RESTART_LOCK_KEY` et file d’attente ou erreur `409`.
- **Durée** : le mode Stealthy peut nécessiter plus de RAM / images ; le redémarrage peut être plus long — timeout et UX claire (§9.3).
- **Jobs interrompus** : politique de reprise ou message « crawl interrompu par redémarrage moteur » ; documenter dans le runbook admin (§7.3).

---

## 11. L’option « redémarrage du moteur » est-elle pertinente ?

**Oui, comme option pragmatique**, tant que `USE_STEALTHY` reste une constante chargée à l’import du worker.

**Avantages**

- Cohérence forte : **aucun décalage** entre ce que l’UI affiche et ce que le process exécute.
- Pas de refactor lourd du `fetcher` ni de risque de mélanger deux modes dans le même process (caches, threads navigateur, etc.).
- Aligné sur l’existant : Docker `Dockerfile.worker.slim` fixe déjà `SILO_USE_STEALTHY_FETCHER=false` au niveau image.

**Inconvénients**

- **Latence** et **complexité d’orchestration** (Compose, K8s, droits API).
- **Coupure** des tâches en cours si pas de drain fin.
- Peu élégant si les besoins deviennent « **chaque projet** son mode sans impacter les autres ».

**Alternative à moyen terme**

- **Deux workers** (file Redis distincte ou tag de job `fetch_mode=stealthy|http`) : pas de restart, mais **double infra** et routage des jobs.

**Conclusion** : pour un premier livrable, **redémarrage ciblé + lancement du crawl dans la foulée** reste une **bonne option** si la latence et le runbook sont acceptés. Sinon migrer vers deux `Deployment` ou refactor **par job** du fetcher.

---

## 12. Critères d’acceptation (récapitulatif)

1. **Back** : `CrawlConfig` étendu ; alignement worker avant enqueue ; erreurs documentées ; Redis runtime si instrumenté.
2. **API config** : le Front peut afficher défaut, options et état runtime.
3. **Admin** : politique d’accès et audit documentés ; runbook Docker/K8s.
4. **Front** : sélecteur de mode, états de chargement, gestion timeout/async, accessibilité, types TS.
5. Aucun crawl lancé si alignement impossible après timeout.
6. Champ crawl **optionnel** : absence de régression sur les clients existants.

---

## 13. Références code

- `main.py` — `CrawlConfig`, `CrawlConfigResponse`, `POST /api/projects/{project_id}/crawl`, `GET /api/config`.
- `worker/fetcher.py` — `SILO_USE_STEALTHY_FETCHER`, `SILO_USE_ASYNC_FETCH`.
- `documentation/ANALYSE_OPTIMISATION_VITESSE_CRAWLER_2026-03-11.md` — recommandations Trafilatura vs Stealthy.
- `Dockerfile.worker` / `Dockerfile.worker.slim` — images worker.
