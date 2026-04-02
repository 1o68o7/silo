# Runbook — migration `opportunity_records` & déploiement API Silo

## 1. Migration PostgreSQL

Script : **`scripts/migrate_opportunity_records_implemented.sql`**.

### Exemple (stack `log8ot`)

```bash
docker exec -i log8ot-silo-db psql -U admin -d semantic_cocoon \
  < /chemin/absolu/vers/silo/scripts/migrate_opportunity_records_implemented.sql
```

**Sortie attendue :** `ALTER TABLE` ×4, `DELETE N`, `CREATE INDEX`.

Adapter conteneur, utilisateur et base. **Ordre :** migration avant ou avec le déploiement du code qui lit ces colonnes.

---

## 2. Rebuild image API (compose parent `log8ot`)

```bash
cd /chemin/vers/log8ot
docker compose build silo
docker compose up -d silo
```

Contrôle OpenAPI : présence de `.../opportunities/summary` et `patch` sur `.../records/{record_id}`.

---

## 3. Front — JWT sur GET

Si **`SILO_REQUIRE_AUTH=true`** :

- `GET /api/projects/{id}/opportunities/records`
- `GET /api/projects/{id}/opportunities/summary`

→ même **`Authorization: Bearer`** que pour `save` / `PATCH` / `DELETE`.

---

## 4. Référence contrat API

**`documentation/BRIEF_REPONSE_OPPORTUNITIES_EXPLORER_2026-04-02.md`**

---

*Version courte. Dépôt **`log8ot`** : runbook long + **miroir SQL** `documentation/silo/scripts/migrate_opportunity_records_implemented.sql` (aligner sur ce fichier `scripts/` en cas de divergence).*
