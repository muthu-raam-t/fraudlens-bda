# Git + SSH setup (passphrase-protected key)

## 1. Confirm which key you already have

```bash
ls -l ~/.ssh/
ssh-keygen -l -f ~/.ssh/id_ed25519.pub    # or id_rsa.pub
```

If a key already exists, reuse it — do not generate a new one.

## 2. Load the key once per login session

Because the key has a passphrase, every `git push` will ask for it unless the
key is held by `ssh-agent`. Load it once:

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
# type the passphrase once
ssh-add -l          # confirm it is loaded
```

Test against GitHub:

```bash
ssh -T git@github.com
# expected: "Hi <username>! You've successfully authenticated..."
```

## 3. Stop typing the passphrase every session

Add this to `~/.ssh/config` so the agent caches the key automatically:

```
Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519
    AddKeysToAgent yes
    IdentitiesOnly yes
```

On Ubuntu with a desktop session, `gnome-keyring` normally holds the passphrase
after the first unlock. If you work in a bare terminal, add to `~/.bashrc`:

```bash
if [ -z "$SSH_AUTH_SOCK" ]; then
  eval "$(ssh-agent -s)" > /dev/null
  ssh-add ~/.ssh/id_ed25519 2>/dev/null
fi
```

## 4. Create the repo and push

Create an **empty** repo named `fraudlens-bda` on GitHub — no README, no
`.gitignore`, no license. Then:

```bash
cd ~/fraudlens-bda
git init
git branch -M main
git config user.name  "Prapul Chandra"
git config user.email "prapulchandra6@gmail.com"

git add .
git commit -m "Phase 0-2: cluster scaffold, Hadoop configs, dataset generator"

git remote add origin git@github.com:<your-username>/fraudlens-bda.git
git push -u origin main
```

Use the **SSH** URL (`git@github.com:...`), not the HTTPS one — HTTPS ignores
your key and asks for a token instead.

## 5. Verify nothing large slipped in

```bash
git count-objects -vH        # size-pack should be well under 1 MB
git ls-files | grep -iE '\.csv|\.parquet' || echo "clean - no data files tracked"
```

GitHub rejects any file over 100 MB, so this check matters before your first
push. If a CSV was staged by accident:

```bash
git rm --cached data/raw/chunk_1.csv
git commit --amend --no-edit
```

## 6. Suggested commit-per-phase history

One commit per phase gives you a clean history to show in the review:

| Commit message | Phase |
|---|---|
| `Phase 0-2: cluster scaffold, Hadoop configs, dataset generator` | now |
| `Phase 3: Hadoop Streaming MapReduce job (fraud rate by state)` | next |
| `Phase 4: Scala preprocessing pipeline on Spark/YARN` | |
| `Phase 5: PySpark MLlib training (LR, RF, GBT)` | |
| `Phase 6: performance tuning benchmarks` | |
| `Phase 7: CatBoost serving model + SHAP` | |
| `Phase 8: dashboard aggregates` | |
| `Phase 9: FastAPI inference service` | |
| `Phase 10: React dashboard` | |
| `Phase 11: Structured Streaming scorer` | |
| `Phase 12: documentation and review deck` | |
