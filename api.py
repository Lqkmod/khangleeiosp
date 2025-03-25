from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/api', methods=['GET'])
def get_data():
    phone = request.args.get('phone')
    soluong = request.args.get('soluong', type=int)

    if not phone or not soluong:
        return jsonify({"status": "error", "message": "Dữ liệu không hợp lệ"}), 400

    data = [{"id": i+1, "phone": phone, "info": f"Dữ liệu mẫu #{i+1}"} for i in range(soluong)]

    return jsonify({"status": "success", "phone": phone, "soluong": soluong, "data": data})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
