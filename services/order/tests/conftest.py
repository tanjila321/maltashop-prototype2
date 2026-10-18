import os
import tempfile

os.environ.update({"AMQP_DISABLED": "1", "SERVICE_NAME": "order",
                   "DB_PATH": os.path.join(tempfile.mkdtemp(), "order.db")})
