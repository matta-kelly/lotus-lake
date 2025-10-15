from flask import Flask
from routes.home import home_bp
from routes.drop_performance import drop_perf_bp

app = Flask(__name__)
app.register_blueprint(home_bp)
app.register_blueprint(drop_perf_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
