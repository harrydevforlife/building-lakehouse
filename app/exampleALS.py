import tensorflow as tf

# Khởi tạo ma trận đặc trưng P và Q
n_users = 100
n_items = 50
n_factors = 10
P = tf.Variable(tf.random.normal([n_users, n_factors], stddev=0.01))
Q = tf.Variable(tf.random.normal([n_items, n_factors], stddev=0.01))

# Hàm tính toán đánh giá của người dùng cho một mặt hàng
def predict_rating(user_id, item_id):
    p_u = P[user_id]
    q_i = Q[item_id]
    return tf.reduce_sum(p_u * q_i)

# Hàm mất mát
def loss_function(user_id, item_id, rating, reg_lambda):
    predicted_rating = predict_rating(user_id, item_id)
    error = rating - predicted_rating
    regularization = reg_lambda * (tf.reduce_sum(tf.square(P[user_id])) + tf.reduce_sum(tf.square(Q[item_id])))
    return tf.square(error) + regularization

# Tạo hàm đánh giá độ chính xác của mô hình
def evaluate_model(test_set):
    error = 0.0
    for user_id, item_id, rating in test_set:
        predicted_rating = predict_rating(user_id, item_id)
        error += tf.square(rating - predicted_rating)
    return error / float(len(test_set))

# Triển khai thuật toán ALS trực tuyến
learning_rate = 0.01
reg_lambda = 0.01
batch_size = 64
optimizer = tf.optimizers.Adam(learning_rate)

for epoch in range(num_epochs):
    # Lấy một lô dữ liệu ngẫu nhiên từ tập dữ liệu
    batch = random.sample(train_set, batch_size)

    # Cập nhật ma trận đặc trưng P
    for user_id, item_id, rating in batch:
        with tf.GradientTape() as tape:
            loss = loss_function(user_id, item_id, rating, reg_lambda)
        gradients = tape.gradient(loss, [P])
        optimizer.apply_gradients(zip(gradients, [P]))

    # Cập nhật ma trận đặc trưng Q
    for user_id, item_id, rating in batch:
        with tf.GradientTape() as tape:
            loss = loss_function(user_id, item_id, rating, reg_lambda)
        gradients = tape.gradient(loss, [Q])
        optimizer.apply_gradients(zip(gradients, [Q]))

    # Đánh giá độ chính xác của mô hình
    train_error = evaluate_model(train_set)
    test_error = evaluate_model(test_set)
    print("Epoch %d: train error = %.4f, test error = %.4f" % (epoch+1, train_error, test_error))