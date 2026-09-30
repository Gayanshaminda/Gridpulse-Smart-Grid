"""Check all three Streamlit pages against the separate offline fixture."""
import os
os.environ["DATABASE_URL"] = "sqlite:///data/preview.db"
os.environ["DEMO_MODE"] = "true"
from streamlit.testing.v1 import AppTest

app = AppTest.from_file("dashboard/app.py", default_timeout=120)
app.run()
assert not app.exception, [e.message for e in app.exception]
assert len(app.metric) == 4
print("Live energy page: passed")
app.sidebar.radio[0].set_value("Daily billing").run()
assert not app.exception, [e.message for e in app.exception]
assert app.metric[0].value == "50"
assert len(app.selectbox[0].options) == 2
print("Daily billing page: passed")
app.sidebar.radio[0].set_value("Pipeline health").run()
assert not app.exception, [e.message for e in app.exception]
assert app.metric[0].value == "18,750"
print("Pipeline health page: passed")
