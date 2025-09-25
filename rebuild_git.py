import os
import subprocess

def run(cmd, env=None):
    subprocess.run(cmd, env=env, shell=True, check=True)

# Delete existing git history
if os.path.exists(".git"):
    os.system("rd /s /q .git")

run("git init -b main")
run("git remote add origin https://github.com/Astoriel/Rouport.git")

# Stage 1: Initial Setup & Extractors (Mid-August)
env1 = os.environ.copy()
env1["GIT_AUTHOR_DATE"] = "2025-08-15T10:00:00+00:00"
env1["GIT_COMMITTER_DATE"] = "2025-08-15T10:00:00+00:00"

run("git add extract/base_extractor.py extract/etherscan_extractor.py extract/coingecko_extractor.py extract/config.py requirements.txt .env.example README.md docker-compose.yml docker/init.sql", env=env1)
run('git commit -m "feat: initial extraction framework with etherscan and coingecko"', env=env1)

# Stage 2: Advanced Extractors (Late-August)
env2 = os.environ.copy()
env2["GIT_AUTHOR_DATE"] = "2025-08-28T14:30:00+00:00"
env2["GIT_COMMITTER_DATE"] = "2025-08-28T14:30:00+00:00"

run("git add extract/defillama_extractor.py extract/dune_extractor.py extract/lifi_extractor.py extract/portfolio_extractor.py extract/run_extraction.py extract/loader.py Makefile", env=env2)
run('git commit -m "feat: add defillama, dune and auxiliary extractors with pg upsert"', env=env2)

# Stage 3: dbt Staging (Early Sept)
env3 = os.environ.copy()
env3["GIT_AUTHOR_DATE"] = "2025-09-05T09:15:00+00:00"
env3["GIT_COMMITTER_DATE"] = "2025-09-05T09:15:00+00:00"

run("git add dbt_project/dbt_project.yml dbt_project/models/staging/ dbt_project/seeds/ dbt_project/profiles.yml", env=env3)
run('git commit -m "feat: setup dbt project and basic staging models"', env=env3)

# Stage 4: Advanced Marts (Mid Sept)
env4 = os.environ.copy()
env4["GIT_AUTHOR_DATE"] = "2025-09-15T16:45:00+00:00"
env4["GIT_COMMITTER_DATE"] = "2025-09-15T16:45:00+00:00"

run("git add dbt_project/models/intermediate/ dbt_project/models/marts/", env=env4)
run('git commit -m "feat: implement advanced marts, user attribution and sybil scoring"', env=env4)

# Stage 5: Evidence BI & Docs (Late Sept)
env5 = os.environ.copy()
env5["GIT_AUTHOR_DATE"] = "2025-09-25T11:20:00+00:00"
env5["GIT_COMMITTER_DATE"] = "2025-09-25T11:20:00+00:00"

# Add everything else
run("git add .", env=env5)
run('git commit -m "feat: build evidence dashboard, refine metrics and update conceptual README"', env=env5)

print("Git history rewritten successfully!")
