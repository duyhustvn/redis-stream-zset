import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomItem } from './k6-utils.js';

const baseUrl = `http://localhost:8081`;

export let options = {
    stages: [
        { duration: '5s', target: 100 },  // Ramp-up 100 user
        { duration: '30s', target: 500 }, // Giữ 500 user
        { duration: '5s', target: 0 },    // Ramp-down
    ],
};


export function setup() {
    let res = http.get(`${baseUrl}/api/test/setup`);
    let valid_sids = [];
    
    if (res.status === 200) {
        try {
            let body = JSON.parse(res.body);
            if (body.sids && body.sids.length > 0) {
                valid_sids = body.sids;
            }
        } catch (e) {
            console.error("[Setup Error] Lỗi parse JSON:", e);
        }
    }
    
    if (valid_sids.length === 0) valid_sids = ["0"];
    return { sids: valid_sids };
}

export default function (data) {
    let current_last_id = randomItem(data.sids);
    let limit = 1500;

    for (let i = 0; i < 10; i++) {
        let url = `${baseUrl}/api/sync?last_id=${current_last_id}&limit=${limit}`;
        let res = http.get(url);

        // 1. Check HTTP Status
        let is200 = check(res, {
            'HTTP status is 200': (r) => r.status === 200,
        });

        if (!is200) {
            // IN LỖI VÀ BỎ QUA LƯỢT NÀY (Thử lại ở vòng lặp tiếp theo với cùng ID)
            console.log(`[HTTP Error] VU: ${__VU}, Iter: ${i}, Status: ${res.status}, Body: ${res.body}`);
            sleep(0.2);
            continue; 
        }

        // 2. Parse JSON
        let body;
        try {
            body = JSON.parse(res.body);
        } catch (e) {
            // IN LỖI PARSE VÀ BỎ QUA
            console.log(`[JSON Error] VU: ${__VU}, Status: ${res.status}, Body: ${res.body}`);
            sleep(0.2);
            continue;
        }
        
        // 3. Check logic nghiệp vụ
        check(body, {
            'Logic is success or up_to_date': (b) => b.status === 'success' || b.status === 'up_to_date',
        });
        
        if (body.status === 'success' && body.events && body.events.length > 0) {
            current_last_id = body.next_last_id; // Cập nhật ID để tiến lên
        } else if (body.status === 'up_to_date') {
            // data đã mới nhất  
            break; 
        }
        sleep(0.2); 
    }
}