# Bonnes pratiques : analyse de silo sémantique à partir d'un crawl technique

**Objectif :** Obtenir des informations suffisantes pour une analyse de silo sémantique SEO via Silo Lab, en partant d'un crawl technique.

---

## 1. Vue d'ensemble du pipeline Silo Lab

```
Crawl technique → Données structurées → Enrichissement → Analyse sémantique → Opportunités
```

| Étape | Données produites | Utilisation |
|-------|-------------------|-------------|
| **Phase 1** | URLs, liens internes, title, H1, content_text | Graphe de liens, Louvain (silos) |
| **Enrichissement** | Métadonnées manquantes, NER (entités) | Qualité, ancres pertinentes |
| **Phase 2** | Embeddings, Reasonable Surfer | Similarité sémantique, opportunités |
| **Recherche** | Paires similaires sans lien | Maillage interne SEO |

---

## 2. Stratégie de crawl : choisir le bon mode

### 2.1 Mode « Liste d'URLs » (CSV / copier-coller)

**Quand l'utiliser :**
- Vous avez déjà un inventaire d'URLs (sitemap, export crawl technique, liste cible).
- Vous voulez contrôler précisément le périmètre.
- Le site est très volumineux et vous visez un échantillon représentatif.

**Bonnes pratiques :**
- Utiliser une liste d'URLs **prioritaires** (pages de conversion, thématiques clés).
- Éviter les doublons (canonical, paramètres).
- Viser **200 à 2000 URLs** pour un bon équilibre qualité / temps de traitement.

**Comportement :** Chaque URL est crawlée seule (`max_depth=0`, `max_pages=1`). Pas de suivi des liens.

---

### 2.2 Mode « Site complet »

**Quand l'utiliser :**
- Premier crawl d'un site pour découvrir la structure.
- Vous voulez le graphe de liens complet (silos basés sur la topologie).

**Bonnes pratiques :**
- Définir une **seed_url** représentative (homepage ou page hub).
- Limiter `max_pages` (ex. 500–1000) pour éviter les dérives.
- Ajuster `max_depth` (2–3) selon la profondeur du site.

**Comportement :** Suit les liens internes depuis la seed jusqu'à `max_pages`.

---

### 2.3 Recommandation hybride

1. **Crawl initial** : liste d'URLs depuis sitemap ou export technique.
2. **Optionnel** : crawl site complet avec `max_pages` modéré pour compléter les liens manquants.
3. **Enrichir** les pages sans métadonnées avant de lancer NER et embeddings.

---

## 3. Qualité des données : prérequis pour l'analyse

### 3.1 Métadonnées obligatoires

| Champ | Rôle | Action si manquant |
|-------|------|---------------------|
| **title** | Identification, ancres | Recrawler (bouton « Recrawler toutes » ou « Crawler cette URL ») |
| **H1** | Thématique, ancres | Idem |
| **content_text** | Embeddings, similarité | NER on-demand tente un fetch si vide |

**Indicateur dans l'outil :** « X pages à enrichir (métadonnées manquantes) ».

### 3.2 Contenu minimal pour les embeddings

- **Minimum :** ~20 caractères de texte extractible.
- **Recommandé :** au moins 100–200 caractères pour une similarité fiable.
- Les pages vides ou quasi-vides sont ignorées pour les opportunités.

### 3.3 Liens internes

- Seuls les liens **internes** (même domaine) sont conservés.
- Chaque lien stocke : anchor, contexte, position (Reasonable Surfer).
- Le graphe de liens sert à Louvain (silos) et à détecter les paires « sans lien ».

### 3.4 URLs avec paramètres (canonicalisation)

- Les URLs contenant des paramètres de requête (`?utm_source=...`, `?fbclid=...`, etc.) sont **filtrées** par défaut.
- **À la découverte** : ces liens ne sont pas ajoutés au crawl (pas de stub, pas d’edge).
- **À l’analyse** : les pages déjà en base avec paramètres sont marquées `excluded` et exclues du graphe, des opportunités et du recrawl batch.
- **Toggle** : sur la page Recherche d’opportunités, un bouton « Afficher » permet d’inclure temporairement les URLs exclues.
- **Règle** : évite les doublons (même contenu, URLs différentes) et le bruit des paramètres de tracking.

---

## 4. Ordre du pipeline recommandé

```
1. Crawl (Phase 1)
   └─ Vérifier : pages crawlées, liens extraits, silos Louvain

2. Enrichir les pages à métadonnées manquantes
   └─ Recrawler les URLs incomplètes

3. NER (optionnel mais utile)
   └─ Améliore les suggestions d'ancres

4. Embeddings
   └─ Nécessaire pour la recherche d'opportunités

5. Recalcul des silos (si graphe modifié)
   └─ Après ajout de pages ou recrawl

6. Recherche d'opportunités
   └─ Seuils 70–90 %, export, enregistrement
```

**Page « Recherche d'opportunités » :** le pipeline (NER, embeddings, recalcul silos) est accessible en un clic.

---

## 5. Paramètres à surveiller

### 5.1 Crawl

| Paramètre | Valeur typique | Impact |
|-----------|----------------|--------|
| max_pages | 500–2000 | Volume, temps, cohérence des silos |
| max_depth | 2–3 | Profondeur explorée |
| run_ner | false (Phase 1) | Phase 2 peut être lancée séparément |

### 5.2 Recherche d'opportunités

| Paramètre | Valeur typique | Impact |
|-----------|----------------|--------|
| min_similarity | 0.75–0.85 | Plus bas = plus d'opportunités, moins précises |
| Seuil 0.90 | Strict | Peu de résultats, très pertinents |
| Seuil 0.70 | Large | Beaucoup de résultats, à trier |

---

## 6. Contrôles qualité dans l'outil

### 6.1 Avant la recherche d'opportunités

- [ ] **Embeddings :** X/Y pages → viser Y/Y (toutes les pages avec contenu).
- [ ] **Pages à enrichir :** 0 (sinon recrawler).
- [ ] **Silos :** au moins 2 communautés Louvain pour une analyse pertinente.

### 6.2 Après la recherche

- [ ] **Similarité max** : si 0 opportunité, baisser le seuil (message dans l'UI).
- [ ] **Zone de texte** : vérifier la pertinence pour l'insertion du lien.
- [ ] **Phrase d'ancrage** : priorité H1 > title > entités NER.

---

## 7. Cas d'usage typiques

### 7.1 Audit silo sur un site existant

1. Exporter les URLs du crawl technique (Screaming Frog, Sitebulb, etc.).
2. Créer un projet Silo avec une seed_url (homepage).
3. Importer les URLs en CSV → « Crawler ces X URL(s) ».
4. Lancer le pipeline : enrichissement → NER → embeddings → recalcul silos.
5. Rechercher les opportunités (seuil 80 %), exporter en CSV/MD.

### 7.2 Découverte complète d'un nouveau site

1. Créer le projet avec la homepage en seed_url.
2. « Démarrer le crawl (site complet) » avec max_pages=500–1000.
3. Enrichir les pages sans métadonnées.
4. Lancer NER, embeddings, recalcul silos.
5. Explorer le graphe et les opportunités.

### 7.3 Maillage ciblé sur une section

1. Filtrer les URLs d'une section (ex. /blog/, /produits/).
2. Importer en liste d'URLs.
3. Crawler uniquement ces URLs.
4. Lancer embeddings + opportunités sur ce sous-ensemble.

---

## 8. Limites et précautions

| Limite | Contournement |
|--------|---------------|
| Sites SPA / JS lourd | Scrapling (StealthyFetcher) utilisé pour le fetch |
| Contenu derrière auth | Non géré, crawler les pages publiques |
| Très gros sites (>5k pages) | Découper par section ou échantillonner |
| Doublons (paramètres, trailing slash) | Normaliser les URLs avant import |

---

## 9. Synthèse : checklist avant analyse

1. **Crawl** : périmètre clair (liste d'URLs ou site complet avec limite).
2. **Enrichissement** : 0 page à enrichir.
3. **Embeddings** : 100 % des pages avec contenu.
4. **Silos** : recalculé si le graphe a changé.
5. **Opportunités** : seuil adapté (80 % en général), export et suivi des actions.
