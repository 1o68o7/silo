# Migration e5-small (Vector 384)

Pour utiliser le modèle **multilingual-e5-small** (2–3× plus rapide, ~60 % moins de RAM) :

1. **Exécuter la migration SQL** (sur une base existante avec Vector 1024) :
   ```bash
   psql $DATABASE_URL -f scripts/migrate_embedding_384.sql
   ```

2. **Configurer l'environnement** :
   ```bash
   export SILO_EMBEDDING_MODEL=intfloat/multilingual-e5-small
   ```

3. **Redémarrer le worker**. Les embeddings seront recalculés au prochain crawl.

**Note :** Pour une installation neuve avec e5-small, la migration n'est pas nécessaire — le schéma utilise déjà la dimension 384.
