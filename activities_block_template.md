
{% for event in events %}
- [ ] {{ event.start_time }}-{{ event.end_time }} [{{ event.event_summary }}](/activity/{{ event.event_id }}.md)
{% endfor %}