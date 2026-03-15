import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomItem } from './k6-utils.js';

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

const baseUrl = `http://localhost:8081`;

// Hàm setup() giữ nguyên như cũ để sinh mảng dynamicIds
// export function setup() {
//     let res = http.get(`${baseUrl}/api/sync?last_id=0`);
//     let ids = ["0"];

//     if (res.status === 200) {
//         let body = JSON.parse(res.body);
//         let events = body.data;
//         if (events.length > 0) {
//             ids.push(events[Math.floor(events.length / 4)].sid.toString());
//             ids.push(events[Math.floor(events.length / 2)].sid.toString());
//             ids.push(events[events.length - 1].sid.toString());
//         }
//     }
//     let lastIdNum = BigInt(ids[ids.length - 1]);
//     ids.push((lastIdNum + 15000n).toString());
//     return ids;
// }

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

// Hàm default được chạy bởi từng VU
export default function (data) {
    let current_last_id = randomItem(data.sids);

    // Lặp 10 lần để lấy cuốn chiếu (tối đa 10 x 15.000 = 150.000 bản ghi)
    for (let i = 0; i < 10; i++) {
        let res = http.get(`${baseUrl}/api/sync?last_id=${current_last_id}`);

        // K6 check HTTP status
        let success = check(res, {
            'status is 200': (r) => r.status === 200,
        });

        // Nếu request lỗi (như server quá tải trả về 500), thoát vòng lặp ngay
        if (!success) {
            break; 
        }

        try {
            let body = JSON.parse(res.body);
            
            // ĐIỀU KIỆN DỪNG: 
            // Nếu mảng rỗng (hoặc trả về ít hơn 15.000, tùy logic), 
            // nghĩa là app đã đồng bộ đến sát hiện tại (Active Events). Không cần lặp tiếp.
            if (!Array.isArray(body) || body.length === 0) {
                break;
            }

            // CẬP NHẬT CURSOR:
            // Dữ liệu từ Redis trả về đã được sort theo SID tăng dần.
            // Do đó, bản ghi cuối cùng trong mảng sẽ chứa SID lớn nhất.
            // Ta lấy SID này làm last_id cho vòng lặp tiếp theo.
            current_last_id = body[body.length - 1].sid;

        } catch (e) {
            console.error("Lỗi parse JSON:", e);
            break;
        }

        // Nghỉ khoảng 200ms - 500ms giữa các lần gọi chunk.
        // Giả lập thời gian client (mobile) parse JSON và insert vào SQLite/Realm nội bộ.
        sleep(Math.random() * 0.3 + 0.2); 
    }

    // Sau khi kết thúc 1 phiên đồng bộ lớn (lấy xong 150k data hoặc đồng bộ xong), 
    // User nghỉ ngơi 1-3 giây trước khi k6 bắt đầu iteration mới.
    sleep(Math.random() * 2 + 1); 
}