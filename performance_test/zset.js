import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomItem } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Cấu hình tải
export const options = {
    vus: 100,        // Giả lập 100 CCU
    duration: '30s', // Chạy trong 30 giây
};

const baseUrl = `http://localhost:8081`;

// 1. GIAI ĐOẠN SETUP: Chuẩn bị kho ID hợp lệ
export function setup() {
    // Gọi API cấp phát ID hợp lệ của server
    let res = http.get(`${baseUrl}/api/test/setup`);
    let valid_sids = [];

    if (res.status === 200) {
        let body = JSON.parse(res.body);
        if (body.sids && body.sids.length > 0) {
            valid_sids = body.sids;
        }
    }

    console.log(`[Setup] Lấy thành công ${valid_sids.length} SID hợp lệ làm mốc xuất phát.`);
    return { sids: valid_sids };
}

// 2. GIAI ĐOẠN CHẠY LOAD TEST
export default function(data) {
    // Mỗi VU (Client) bốc ngẫu nhiên một ID để bắt đầu đồng bộ
    let current_last_id = randomItem(data.sids);
    let limit = 15000;

    // Client chay 10 lan moi lan 15k de lay 150k 
    for (let i = 0; i < 10; i++) {
        let url = `${baseUrl}/api/sync?last_id=${current_last_id}&limit=${limit}`;
        let res = http.get(url);

        check(res, {
            'HTTP status is 200': (r) => r.status === 200,
            'Logic is success or up_to_date': (r) => {
                let body = JSON.parse(r.body);
                return body.status === 'success' || body.status === 'up_to_date';
            },
        });

        let body = JSON.parse(res.body);

        if (body.status === 'success' && body.events && body.events.length > 0) {
            // Lấy ID mới nhất của batch này để làm đà cho vòng lặp tiếp theo
            current_last_id = body.next_last_id;
        } else if (body.status === 'up_to_date') {
            // Đã chạm đến ranh giới Watermark 5 giây. Không còn data mới.
            // Client ngắt vòng lặp kết nối sớm để nhường tài nguyên.
            break;
        } else {
            // Rơi vào các case Out of Sync hoặc lỗi khác
            break;
        }

        // Nghỉ 500ms giữa các lần kéo để mô phỏng thời gian điện thoại insert local DB
        sleep(0.5);
    }
}
