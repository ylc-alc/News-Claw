# News-Claw

News-Claw is a GitHub Pages-based daily news briefing site focused on three areas:

- 科技
- 政治
- 經濟

The site is designed to publish a structured daily briefing in Traditional Chinese, following these editorial principles:

- 專業、冷靜、直接
- 重視脈絡、政策背景與利益關係人
- 避免未經證實消息
- 優先使用可信新聞來源
- 聚焦每日最重要且具延續性的新聞主題

## Planned workflow

This project will gradually be built in phases:

### Phase 1
Static site structure and GitHub Pages setup.

### Phase 2
GitHub Actions workflow setup for manual and scheduled runs.

### Phase 3
RSS source ingestion and data normalisation.

### Phase 4
News classification, de-duplication, and topic selection.

### Phase 5
HTML generation and automated publishing.

## Site structure

- `docs/`: public site files for GitHub Pages
- `config/`: feed and site configuration
- `data/`: raw and processed news data
- `scripts/`: Python scripts for fetching and building content

## Content direction

Each daily update is planned to include:

- 科技新聞
- 政治新聞
- 經濟新聞

Each topic will aim to include:

1. 重點新聞內容
2. 背景說明與脈絡
3. 涉及利益方的角力分析
4. 來源出處
