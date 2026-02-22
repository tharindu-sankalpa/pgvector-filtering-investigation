## Repository Context & Author Intent

This repository supports a technical deep-dive investigation into **PostgreSQL + PGVector filter search performance degradation**.

The primary objectives of this work are:

1. **Showcase real-world technical expertise**
2. Contribute meaningful insights to the open-source community
3. Demonstrate investigative depth and engineering rigor
4. Attract recruiter and community attention through high-quality technical writing
5. Provide reproducible artifacts for practitioners

This repository is tightly coupled with a Medium technical article.

---

## Core Content Strategy

The `README.md` file serves two purposes:

1. It is the **canonical version of the Medium article**
2. It acts as a structured, high-level technical narrative

The README **must remain clean, engaging, and story-driven**.

It should:

- Focus on explanation, insight, and reasoning
- Highlight investigation methodology
- Explain findings clearly
- Avoid overwhelming readers with setup scripts and long command blocks
- Be structured like a polished technical article

---

## Documentation Philosophy

This repository follows a **Layered Documentation Model**:

### Layer 1 - README (Article Layer)

- Conceptual explanations
- Architecture diagrams
- Performance findings
- Root cause analysis
- Benchmark summaries
- Key code snippets (minimal, illustrative only)
- Links to detailed implementations

### Layer 2 - `/docs` Directory (Implementation Layer)

- Full AKS cluster setup
- CloudNativePG installation steps
- Kubernetes manifests
- Helm configurations
- Benchmark scripts
- Full reproducible environment setup
- Deep technical commands and scripts

The README should reference `/docs` when needed using phrasing such as:

> For full AKS cluster setup and CloudNativePG installation, see `/docs/aks-setup.md`.

---

## Writing Guidelines for AI Assistance

When helping with README or article content:

- Keep tone professional but engaging
- Make explanations crisp and high-signal
- Avoid unnecessary verbosity
- Prioritize clarity over jargon
- Preserve technical depth
- Avoid marketing language
- Avoid generic statements
- Avoid fluff

When including code:

- Keep snippets short and illustrative
- Move large scripts to `/docs`
- Link instead of embedding full configurations

---

## Technical Positioning

This work should emphasize:

- Performance investigation methodology
- Query planner behavior
- Index selection impact
- Filter + vector search interaction
- Benchmark comparisons
- Real production-grade thinking

It should read like:

- A senior engineer’s deep-dive analysis
- Not a tutorial for beginners
- Not surface-level documentation

---

## Reproducibility Requirement

Anyone should be able to replicate the investigation by:

1. Reading the README for conceptual understanding
2. Following `/docs` for environment recreation
3. Running provided benchmark scripts

Reproducibility is critical.

---

## Target Audience

- Backend engineers
- Database engineers
- Platform engineers
- AI infrastructure engineers
- Recruiters evaluating deep technical capability
- Open-source contributors

---

## AI Output Expectations

When generating content:

- Maintain consistency with investigation theme
- Ensure technical correctness
- Favor structured sections with headers
- Suggest improvements when clarity is weak
- Do not oversimplify technical concepts
- Keep README Medium-ready
