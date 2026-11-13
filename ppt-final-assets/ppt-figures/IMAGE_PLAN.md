# Image Plan for Review 2

Two kinds of image belong in this deck, and they do different jobs.

- **Evidence images** — screenshots of your own running cluster. These are the ones that prove the system exists. Nobody can fake a Spark DAG. Capture these yourself.
- **Figure images** — charts built from your measured numbers. Five are ready in this folder.

A third category — stock photos of credit cards, padlocks, glowing circuit boards — is the thing to avoid. It adds nothing and reads as filler on a technical deck.

---

## Part A — Screenshots to capture (the strongest images you have)

Bring the cluster up before capturing:

```bash
cd ~/fraudlens-bda
docker compose up -d && sleep 45
docker compose exec -T namenode hdfs dfsadmin -safemode get   # wait for OFF
```

| # | Image | Where to capture | Which slide | Why it matters |
|---|---|---|---|---|
| S1 | **HDFS cluster summary** | `localhost:9870` → Overview | Results · Storage | Shows live DataNode, block count, capacity, zero corrupt blocks — one screenshot evidencing the whole storage claim |
| S2 | **HDFS file browser** | `localhost:9870` → Utilities → Browse the file system → `/dataset/preprocessed/` | Methodology or Results | The `year=/month=` partition directories are visible — this is what makes partition pruning real rather than asserted |
| S3 | **`fsck` output** | Terminal: `docker compose exec -T namenode hdfs fsck /dataset/unprocessed -files -blocks \| tail -25` | Results · Integrity | The word **HEALTHY** with the block count. Crop tight to the summary block |
| S4 | **YARN applications list** | `localhost:8088` → Applications | Implementation | Shows jobs running under YARN as cluster manager, with MapReduce and Spark applications side by side |
| S5 | **Spark job DAG** | `localhost:4040` → Jobs → click a job → DAG Visualization | Analysis | The single most impressive screenshot available. Stage boundaries are visibly where shuffles occur |
| S6 | **Spark stages, showing skipped** | `localhost:4040` → Stages | Analysis | Your "108 of 345 stages skipped" claim, visible on screen |
| S7 | **Structured Streaming tab** | `localhost:4040` → Structured Streaming | Results · Streaming | Micro-batch timeline with input rate and processing time — proves the stream is live, not simulated |
| S8 | **Live operations dashboard** | `localhost:8501` during a demo run | Implementation / Conclusion | Your own UI, mid-flow, with transactions being scored |
| S9 | **Terminal: consumer batches scrolling** | The `demo_live.sh` terminal | Streaming slide | Lines like `batch 10 \| 16 events \| 2 flagged` — raw, credible, unpolished in a good way |
| S10 | **Docker Compose service list** | Terminal: `docker compose ps` | Implementation | Nine services up. Evidences "self-provisioned stack" in one frame |

**Capture tips**

- Use a light browser theme for S1–S7 if your slide background is light; a dark UI screenshot on a white slide looks pasted on. S8 and S9 are exceptions — keep those dark, they're meant to look like an ops console.
- Crop hard. A full browser window wastes 40% of the slide on chrome and tabs. Capture only the panel that carries the number.
- On Ubuntu: `Shift + PrtSc` gives you a region selection.
- Zoom the browser to 125–150% before capturing. Screenshots get scaled *down* on a slide, and default-size text becomes unreadable from the back of a room.
- Never screenshot a terminal with your home directory path visible if you'd rather not show it — `clear` first, then run the one command.

---

## Part B — Figures ready in this folder

| File | Contents | Suggested slide |
|---|---|---|
| `fig0_architecture.png` | Your existing full architecture diagram | Methodology / System Architecture |
| `fig1_engine_benchmark.png` | MapReduce 613 s vs Spark 65.3 s, with the cross-validation note | Results · Engine benchmark |
| `fig2_model_benchmark.png` | PR-AUC for GBT / RF / LR against the random baseline | Results · Model benchmark |
| `fig3_optimisation.png` | Partition pruning 11.73×, broadcast 3.79×, caching 1.22× | Results · Optimisation, or Analysis |
| `fig4_data_funnel.png` | 28.1 GB → 6.6 GB → 18.7 KB, three-stage flow | Introduction or Methodology |
| `fig5_class_imbalance.png` | 1,000 squares, 2 of them fraud | Problem Statement |

All five are 200 dpi on a near-white surface (`#fcfcfb`), so they sit cleanly on a light slide. Drop them in at full width — don't stretch them to fit a box, the aspect ratio is already chosen for a 16:9 slide.

If your deck is dark-themed, tell me and I'll re-render all five against a dark surface rather than you placing light rectangles on a dark background.

---

## Part C — Suggested image per slide

| Slide | Image |
|---|---|
| 1 · Title | None, or S8 cropped as a faint background strip |
| 2 · Introduction | `fig4_data_funnel.png` |
| 3 · Problem Statement | `fig5_class_imbalance.png` |
| 4 · Objectives | None — text slide, let it breathe |
| 5 · Literature Survey | None |
| 6 · Methodology | `fig0_architecture.png`, with S2 as a small inset if space allows |
| 7 · Implementation | S10 and S4 side by side, or S8 full-width |
| 8 · Results | `fig1` + `fig2` on one slide; S1 and S3 on a second results slide |
| 9 · Analysis | S5 (the DAG) full-width with `fig3` beside it |
| 10 · Big Data Utilisation | S6, showing skipped stages |
| 11 · Conclusion | S7 or S8 — end on the system running |

---

## Two things worth being careful about

**Don't put a chart and a table of the same numbers on one slide.** Pick one. The chart is better for the engine benchmark and the model comparison; the table is better for the storage and integrity figures, where the precision is the point.

**Label every screenshot.** A bare screenshot forces the audience to hunt for what matters. Put a one-line caption under each — *"HDFS NameNode: 230 blocks, 1 live DataNode, zero corrupt blocks"* — or drop a coloured rectangle outline on the one number you want them to look at. Faculty scan slides in about four seconds; the caption is what they actually read.
