# Gold Standard Set

Hand-coded ground truth for pipeline validation. One YAML file per case.

## Scope (v1.0.0 — Option B)

60 cases weighted toward Burger–Roberts era (1969–present), plus 10 landmark pre-1950 cases.
Full 100-case set in v1.1.0.

## File format

Each file: `cases/{scdb_case_id}.yaml` or `cases/{year}_{party_v_party_slug}.yaml`

```yaml
case_id: 12345              # FLexlaw case ID
scdb_case_id: "1999-082"
oyez_case_id: "grutter-v-bollinger"
case_name: "Grutter v. Bollinger"
decision_date: "2003-06-23"
term: 2002                  # OT year
majority_size: 5
chief_justice_era: "Rehnquist"
disposition: "affirmed"

opinions:
  - opinion_id: 99999
    opinion_type: majority
    author: "Sandra Day O'Connor"
    joiners:
      - "John Paul Stevens"
      - "David H. Souter"
      - "Ruth Bader Ginsburg"
      - "Stephen G. Breyer"

  - opinion_id: 99998
    opinion_type: concurrence
    author: "Ruth Bader Ginsburg"
    joiners:
      - "Stephen G. Breyer"

  - opinion_id: 99997
    opinion_type: dissent
    author: "William H. Rehnquist"
    joiners:
      - "Antonin Scalia"
      - "Clarence Thomas"

  - opinion_id: 99996
    opinion_type: dissent
    author: "Clarence Thomas"
    joiners:
      - "William H. Rehnquist"
      - "Antonin Scalia"

votes:
  - justice: "Sandra Day O'Connor"
    vote: majority
    authored_opinion: 99999
  - justice: "Ruth Bader Ginsburg"
    vote: majority
    authored_opinion: 99998
    joined_opinions: [99999]
  # ... etc.

primary_provisions:
  - amend.14.s1.equal_protection

primary_doctrines:
  - strict_scrutiny_ep

notes: |
  Diversity rationale upheld; narrow tailoring required.
  O'Connor majority; Rehnquist, Kennedy, Scalia, Thomas dissent.
  Companion case: Gratz v. Bollinger (undergraduate admissions, struck down).
  Coded_by: [initials]
  Coded_date: YYYY-MM-DD
```

## Coding instructions

See `gold/CODING_GUIDE.md` (to be written in Phase 0).

Key rules:
1. `opinion_type` must be exactly one of: majority, dissent, concurrence, mixed, per_curiam, plurality, other
2. `joiners` lists justices who joined but did NOT author the opinion
3. `vote` in the votes section reflects the justice's vote on the *judgment*, not which opinion they joined
4. A justice who authors a concurrence in the judgment but doesn't join the majority has `vote: majority` (they're in the judgment coalition) and `authored_opinion: [concurrence id]`
5. For plurality cases, `vote: majority` means voted for the judgment; separately note which opinion they joined
