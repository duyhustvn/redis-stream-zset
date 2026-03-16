import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomItem } from './k6-utils.js';

const baseUrl = `http://localhost:8081`;

export const options = {
  stages: [
    { duration: '20s', target: 100 }, // Step 1: Ramp to 100 VUs
    { duration: '30s', target: 100 }, // Hold at 100 VUs (Observe baseline)
    { duration: '20s', target: 300 }, // Step 2: Ramp to 300 VUs
    { duration: '30s', target: 300 }, // Hold at 300 VUs (Does latency spike here?)
    { duration: '20s', target: 500 }, // Step 3: Ramp to 500 VUs
    { duration: '30s', target: 500 }, // Hold at 500 VUs (Peak load)
    { duration: '20s', target: 0 },   // Graceful scale down
  ],
//   thresholds: {
//     // This will fail the test if 95% of requests take longer than 3 seconds
//     http_req_duration: ['p(95)<3000'], 
//   },
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
    let limit = 15000;

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
            continue; 
        }

        // 2. Parse JSON
        let body;
        try {
            body = JSON.parse(res.body);
        } catch (e) {
            // IN LỖI PARSE VÀ BỎ QUA
            console.log(`[JSON Error] VU: ${__VU}, Status: ${res.status}, Body: ${res.body}`);
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