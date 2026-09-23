NEXUS shadow research: no changes to the bot or its paper/manual setting.

Place shadow_research.py in /home/Hdez98/nexus, alongside paper_trades.json
and research_data/. In a PythonAnywhere Bash console run:

cd /home/Hdez98/nexus
python3 shadow_research.py

The script reads settled V5.8 markets and compares four fixed signals at the
first logged quote with 330-360 seconds left. Historical discovery includes
markets through 2026-09-23 01:15 UTC; subsequent markets appear under FUTURE
HOLDOUT. These fixed rules must not be tuned against the holdout as it grows.

The estimated P/L assumes buying one contract at the saved ask and holding
to settlement. It is not an executed trade record; fees may differ by series,
the snapshot may not reflect available size, and a manual order can fill later
at a different price. No order entry, history edits, or site reload is needed.
