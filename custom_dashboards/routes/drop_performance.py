from flask import Blueprint, render_template

drop_perf_bp = Blueprint("drop_performance", __name__)

@drop_perf_bp.route("/drop-performance")
def drop_performance():
    return render_template("drop_performance.html", title="Drop Performance")
