package com.signomix.common.iot.tts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializer {

    public static void main(String[] args) {
        // Przykładowy JSON z The Things Stack z danymi rx_metadata
        String jsonUplink = "{\n" +
            "    \"end_device_ids\": {\n" +
            "        \"device_id\": \"eui-24e124468d355937\",\n" +
            "        \"application_ids\": { \"application_id\": \"smart-sense-app\" },\n" +
            "        \"dev_eui\": \"24E124468D355937\"\n" +
            "    },\n" +
            "    \"received_at\": \"2025-07-05T10:56:09.934296363Z\",\n" +
            "    \"uplink_message\": {\n" +
            "        \"f_port\": 85,\n" +
            "        \"frm_payload\": \"/xkABAWaGXJD/xkBBAWaGXpD/xkCBAUzs3RD/xkDBAV7FEhC/xkEBAUK1yM+/xkFBAUK1yM8/xkGBAUAAEA//xkHBAV3vh8+/xkIBAWmm0Q7/xkJBAVvEgM7/xkKBAWamRk+/xkLBAUpWGFF/xkMBAWFqzFE/xkNBAUpLKNE/xkOBAVmrsZE\",\n" +
            "        \"decoded_payload\": { \"chn1\": 242.1, \"chn2\": 250.1 },\n" +
            "        \"rx_metadata\": [\n" +
            "            {\n" +
            "                \"gateway_ids\": {\n" +
            "                    \"gateway_id\": \"my-ttig-1\",\n" +
            "                    \"eui\": \"58A0CBFFFE8013A9\"\n" +
            "                },\n" +
            "                \"time\": \"2025-07-05T10:56:09.591160058Z\",\n" +
            "                \"timestamp\": 1506203763,\n" +
            "                \"rssi\": -68,\n" +
            "                \"channel_rssi\": -68,\n" +
            "                \"snr\": 9.0,\n" +
            "                \"location\": {\n" +
            "                    \"latitude\": 51.57426725879415,\n" +
            "                    \"longitude\": 19.261481030328895,\n" +
            "                    \"source\": \"SOURCE_REGISTRY\"\n" +
            "                },\n" +
            "                \"received_at\": \"2025-07-05T10:56:09.654354446Z\"\n" +
            "            }\n" +
            "        ],\n" +
            "        \"settings\": {\n" +
            "            \"data_rate\": { \"lora\": { \"bandwidth\": 125000, \"spreading_factor\": 7 } },\n" +
            "            \"frequency\": \"868100000\"\n" +
            "        },\n" +
            "        \"received_at\": \"2025-07-05T10:56:09.727190828Z\"\n" +
            "    }\n" +
            "}";

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            // Deserializacja JSONa
            UplinkMessage uplinkMessage = objectMapper.readValue(jsonUplink, UplinkMessage.class);

            // Wyświetlenie całego zdeserializowanego obiektu
            System.out.println("✅ Pomyślnie zdeserializowano JSON:");
            System.out.println(uplinkMessage);
            
            // Dostęp do danych z rx_metadata
            // Sprawdzamy, czy lista nie jest pusta, aby uniknąć błędu
            if (uplinkMessage.getUplinkPayload().getRxMetadata() != null && !uplinkMessage.getUplinkPayload().getRxMetadata().isEmpty()) {
                
                // Pobieramy pierwszy element z listy metadanych (zazwyczaj jest jeden, ale może być więcej)
                RxMetadata firstGatewayMetadata = uplinkMessage.getUplinkPayload().getRxMetadata().get(0);

                String gatewayId = firstGatewayMetadata.getGatewayIds().getGatewayId();
                int rssi = firstGatewayMetadata.getRssi();
                double snr = firstGatewayMetadata.getSnr();
                double latitude = firstGatewayMetadata.getLocation().getLatitude();
                double longitude = firstGatewayMetadata.getLocation().getLongitude();

                System.out.println("\n📡 Odczytane dane z rx_metadata:");
                System.out.println("ID bramki: " + gatewayId);
                System.out.println("RSSI: " + rssi + " dBm");
                System.out.println("SNR: " + snr + " dB");
                System.out.println("Szerokość geograficzna: " + latitude);
                System.out.println("Długość geograficzna: " + longitude);
            }

        } catch (JsonProcessingException e) {
            System.err.println("❌ Błąd podczas deserializacji JSON: " + e.getMessage());
        }
    }
}
