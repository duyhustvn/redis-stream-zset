import http from 'k6/http';
import { check, sleep } from 'k6';
// Import thư viện randomItem có sẵn của k6
import { randomItem } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

export const options = {
    vus: 500,
    duration: '30s',
};

// 1. GIAI ĐOẠN SETUP (Chạy 1 lần duy nhất bởi 1 thread)
export function setup() {
    // Đưa vào 1 last_id hợp lệ (có thể lấy từ /api/init) làm mốc ban đầu
    let base_sid = "115199251122159880"; // <--- BẠN NHỚ THAY SỐ NÀY

    // Kéo trước 2000 bản ghi để tạo thành một "kho" ID hợp lệ
    let url = `http://localhost:8080/api/sync?last_id=${base_sid}&limit=2000`;
    let res = http.get(url);

    let valid_sids = [base_sid]; // Dự phòng nếu gọi lỗi thì vẫn có 1 ID

    if (res.status === 200) {
        let body = JSON.parse(res.body);
        if (body.events && body.events.length > 0) {
            // Trích xuất toàn bộ trường 'sid' (uint64) từ mảng events
            valid_sids = body.events.map(e => e.sid);
        }
    }

    console.log(`[Setup] Đã chuẩn bị được ${valid_sids.length} ID động để test.`);

    // Dữ liệu return ở đây sẽ được truyền vào biến 'data' của hàm default
    return { sids: valid_sids };
}

// 2. GIAI ĐOẠN CHẠY TEST (Chạy song song bởi 500 VUs)
export default function(data) {
    // Mỗi User sẽ bốc ngẫu nhiên một mốc thời gian (SID) từ kho dữ liệu
    let current_last_id = randomItem(data.sids);
    let limit = 1000;

    for (let i = 0; i < 5; i++) {
        let url = `http://localhost:8081/api/sync?last_id=${current_last_id}&limit=${limit}`;
        let res = http.get(url);

        check(res, {
            'status is 200': (r) => r.status === 200,
            'is success or up_to_date': (r) => {
                let body = JSON.parse(r.body);
                return body.status === 'success' || body.status === 'up_to_date';
            },
        });

        let body = JSON.parse(res.body);
        if (body.status === 'success' && body.next_last_id) {
            current_last_id = body.next_last_id;
        } else {
            break; // Hết data mới thì thoát sớm, đỡ gọi thừa
        }

        sleep(0.5);
    }
}
