# AI usage

Keep all three headings exactly as they are. If this file is missing or any heading is
missing, the grader stops and gives 0 ("AI_USAGE.md missing or incomplete"). The content
is not scored, but your Track Mentor reads it and the W8 defence can ask about it.

## Tools

I used opencode (deepseek-v4.1-flash-free) to read the task, plan the subtasks, implement
`app/budget.py`, `app/stream.py`, and `app/main.py` and run the public tests plus local
ad-hoc checks.

## What AI got wrong

The assistant initially planned to settle a provider that failed before its first token with
`actual_cost_usd(provider, prompt, 0)`. That would have charged the prompt's input tokens
even though the task says those attempts cost nothing. I caught it by re-reading the README
and checking the budget after a fallback in a local test, then changed the stream to settle
`0.0` when no token was produced. 

A smaller fix: the first `try_reserve` implementation called `today()` twice, so it could check one date and record spend on another; it now reads the date once.

## In my own words

<!-- Answer without AI, one or two sentences each. -->

1. Why is it safe to switch provider before the first token and not after?

Because once streaming starts the user already can see the output. So, if we switch providers he can see an incomplete output then a duck taped output from another provider. Before the first token, the user sees nothing. So, switching providers is safe.

2. Which checks did you put before the stream starts, and why could they not be error events instead?

Before the stream starts I check for the cheapest provider’s worst-case estimate and the clause text (Like language and the length). Error events are for things that happen after streaming starts like a network error or a provider's error. The checks must be answered before the stream starts because starting it fixes the status at 200.