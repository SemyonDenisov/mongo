package ru.samis.harvesters;


import com.google.common.io.CharStreams;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.model.Indexes;
import org.apache.commons.codec.Charsets;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;


public class ReformaGKHharvester extends Harvester {
    private final HashMap<String, String> fieldMap = new HashMap<String, String>() {{
        put("Тип фундамента ", "foundationType");
        put("Тип внутренних стен", "wallMaterial");
        put("Общая площадь жилых помещений", "areaResidential");
        put("Общая площадь нежилых помещений, за исключением помещений общего пользования", "areaNonResidential");
        put("Общая площадь помещений общего пользования в многоквартирном доме", "areaCommonProperty");
        put("Количество подъездов в многоквартирном доме", "entranceCount");
        put("Количество лифтов ", "elevatorsCount");
        put("Количество жилых помещений (квартир)", "livingFlatsCount");
        put("Количество нежилых помещений", "unlivingFlatsCount");
        put("Количество этажей", "floorsCount");
        put("Тип внутридомовой системы отопления", "heatingType");
        put("Тип внутридомовой инженерной системы холодного водоснабжения", "coldWaterType");
        put("Тип внутридомовой инженерной системы горячего водоснабжения", "hotWaterType");
        put("Тип внутридомовой инженерной системы водоотведения", "sewerageType");
        put("Количество вводов внутридомовой инженерной системы электроснабжения в многоквартирный дом (количество точек поставки)", "electricalEntriesCount");
        put("Тип внутридомовой инженерной системы газоснабжения", "gasType");
    }};


    private static Document getField(JsonElement field) {
        try {
            return new Document().append("value", field.getAsInt());
        } catch (Exception ignored) {
        }
        try {
            return new Document().append("value", field.getAsDouble());
        } catch (Exception ignored) {
        }
        try {
            return new Document().append("value", field.getAsString());
        } catch (Exception ignored) {
        }
        return new Document().append("value", null);
    }

    private Header[] getHeaders() {
        return new Header[]{
                new BasicHeader("Host", "dom.gosuslugi.ru")
                , new BasicHeader("State-Guid", "/houses")
                , new BasicHeader("Sec-Ch-Ua", "\"Chromium\";v=\"123\", \"Not:A-Brand\";v=\"8\"")
                , new BasicHeader("Sec-Ch-Ua-Platform", "Windows")
                , new BasicHeader("Sec-Ch-Ua-Mobile", "?0")
                , new BasicHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36")
                , new BasicHeader("Content-Type", "application/json;charset=UTF-8")
                , new BasicHeader("Session-Guid", UUID.randomUUID().toString())
                , new BasicHeader("Request-Guid", UUID.randomUUID().toString())
                , new BasicHeader("Origin", "https://dom.gosuslugi.ru")
                , new BasicHeader("Sec-Fetch-Site", "same-origin")
                , new BasicHeader("Sec-Fetch-Mode", "cors")
                , new BasicHeader("Sec-Fetch-Dest", "empty")
                , new BasicHeader("Referer", "https://dom.gosuslugi.ru/")
                , new BasicHeader("Accept-Encoding", "gzip, deflate, br")
                , new BasicHeader("Accept-Language", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
                , new BasicHeader("Priority", "u=1, i")
                , new BasicHeader("Connection", "close")

        };
    }

    private String buildURL(String baseUrl, String regionCode, String cityCode, String areaCode, String settlementCode, String streetCode, String elementCode, int itemsPerPage) {
        baseUrl += "&itemsPerPage=" + itemsPerPage;
        if (regionCode != null) {
            baseUrl += "&regionCode=" + regionCode;
        }
        if (cityCode != null) {
            baseUrl += "&cityCode=" + cityCode;
        }
        if (settlementCode != null) {
            baseUrl += "&settlementCode=" + settlementCode;
        }
        if (areaCode != null) {
            baseUrl += "&areaCode=" + areaCode;
        }
        if (streetCode != null) {
            baseUrl += "&streetCode=" + streetCode;
        }
        if (elementCode != null) {
            baseUrl += "&planningStructureElementCode=" + elementCode;
        }
        return baseUrl;
    }

    private String POST(String url, String body) throws IOException {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Host,Content-Length,Connection");
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(url);
        request.setHeaders(getHeaders());
        request.setEntity(new StringEntity(body));
        try {
            return client.execute(request, response -> {
                if (response.getStatusLine().getStatusCode() == 200) {
                    InputStream is = response.getEntity().getContent();
                    return CharStreams.toString(new InputStreamReader(
                            is, Charsets.toCharset("UTF-8")));
                }
                return null;
            });
        } catch (java.io.IOException e) {
            writeError(e.getMessage());
        } finally {
            client.close();
        }
        return null;
    }

    private String GET(String url) throws IOException {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Host,Content-Length,Connection");
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);
        request.setHeaders(getHeaders());
        try {
            return client.execute(request, response -> {
                if (response.getStatusLine().getStatusCode() == 200) {
                    InputStream is = response.getEntity().getContent();
                    return CharStreams.toString(new InputStreamReader(
                            is, Charsets.toCharset("UTF-8")));
                }
                return null;
            });
        } catch (java.io.IOException e) {
            writeError(e.getMessage());
        } finally {
            client.close();
        }
        return null;
    }

    private ArrayList<String> getCodes(String url) throws IOException {
        String response = null;
        while (response == null) {
            response = GET(url);
            if (!Objects.equals(response, "[]") && response != null) {
                try {
                    ArrayList<String> codes = new ArrayList<>();
                    JsonArray responseJson = JsonParser.parseString(response).getAsJsonArray();
                    responseJson.forEach(item ->
                            codes.add(item.getAsJsonObject().get("aoGuid").getAsString()));
                    return codes;
                } catch (RuntimeException e) {
                    writeError(e.getMessage());
                }
                return new ArrayList<>();
            }
        }

        return new ArrayList<>();
    }

    private String getHousesInPage(String regionCode, String cityCode, String streetCode, String areaCode, String settlementCode, String elementCode, int pageIndex, int elementsPerPage) throws IOException {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Host,Content-Length,Connection");
        String json = "{\"fiasHouseCodeList\":null,\"estStatus\":null," +
                "\"strStatus\":null,\"calcCount\":true,\"houseConditionRefList\":null," +
                "\"houseTypeRefList\":[\"1\"],\"houseManagementTypeRefList\":null," +
                "\"cadastreNumber\":null,\"oktmo\":null,\"statuses\":[\"APPROVED\"],\"regionProperty\":null,\"municipalProperty\":null,\"hostelTypeCodes\":null";
        if (cityCode != null) {
            json += ", \"cityCode\": \"" + cityCode + "\"";
        }
        if (regionCode != null) {
            json += ", \"regionCode\": \"" + regionCode + "\"";
        }
        if (settlementCode != null) {
            json += ", \"settlementCode\": \"" + settlementCode + "\"";
        }
        if (areaCode != null) {
            json += ", \"areaCode\": \"" + areaCode + "\"";
        }
        if (streetCode != null) {
            json += ", \"streetCode\": \"" + streetCode + "\"";
        }
        if (elementCode != null) {
            json += ", \"planningStructureElementCode\": \"" + elementCode + "\"";
        }
        json += "}";
        String url = String.format("https://dom.gosuslugi.ru/homemanagement/api/rest/services/houses/public/searchByAddress?pageIndex=%d&elementsPerPage=%d", pageIndex, elementsPerPage);
        return POST(url, json);
    }

    private JsonArray getHousesInStreet(String regionCode, String cityCode, String areaCode, String settlementCode, String streetCode, String elementCode, int elementsPerPage) throws IOException, InterruptedException {

        boolean flag = true;
        int pageIndex = 1;
        JsonArray houses = new JsonArray();
        while (flag) {
            String response = getHousesInPage(regionCode, cityCode, streetCode, areaCode, settlementCode, elementCode, pageIndex, elementsPerPage);
            if (response != null) {
                try {
                    JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
                    JsonArray items = responseJson.getAsJsonArray("items");
                    if (items.size() > 0) {
                        responseJson.getAsJsonArray("items").forEach(houses::add);
                        pageIndex++;
                    } else {
                        flag = false;
                    }
                } catch (RuntimeException e) {
                    writeError(e.getMessage());
                }
            } else if (response == null) {
                Thread.sleep(20000);
            }
        }
        return houses;
    }

    private ArrayList<String> requestObjects(String baseurl, int itemsPerPage, String regionCode, String areaCode, String cityCode, String settlementCode, String elementCode) throws IOException {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Host,Content-Length,Connection");
        boolean flag = true;
        int pageIndex = 1;
        ArrayList<String> codes = new ArrayList<>();
        while (flag) {
            String url = buildURL(baseurl, regionCode, cityCode, areaCode, settlementCode, null, elementCode, itemsPerPage);
            url += "&page=" + pageIndex;
            ArrayList<String> codesInPage = getCodes(url);
            if (codesInPage.size() > 0) {
                codes.addAll(codesInPage);
                pageIndex++;
            } else {
                flag = false;
            }
        }
        return codes;
    }

    private Document getInfoAboutHouse(JsonObject houseJson) throws InterruptedException, IOException {
        try {
            JsonObject houseAddressJson = houseJson.getAsJsonObject("address");
            Document house = new Document();
            house.put("Region", getField(houseAddressJson.getAsJsonObject("region").get("formalName")).get("value"));
            house.put("RegionID", getField(houseAddressJson.getAsJsonObject("region").get("aoGuid")).get("value"));
            house.put("RegionCode", getRegionCode());
            house.put("PostalCode", getField(houseAddressJson.getAsJsonObject("region").get("postalCode")).get("value"));
            if (houseAddressJson.get("area").isJsonNull()) {
                house.put("Municipalitet", getField(houseAddressJson.getAsJsonObject("city").get("shortName")).get("value") + " " +
                        getField(houseAddressJson.getAsJsonObject("city").get("formalName")).get("value"));
                house.put("BlockID", getField(houseAddressJson.getAsJsonObject("city").get("formalName")).get("value"));

            } else {
                house.put("BlockID", getField(houseAddressJson.getAsJsonObject("area").get("formalName")).get("value"));
                house.put("MunicipalitetID", getField(houseAddressJson.getAsJsonObject("settlement").get("aoGuid")).get("value"));
                house.put("Municipalitet",
                        getField(houseAddressJson.getAsJsonObject("area").get("shortName")) + " " +
                                getField(houseAddressJson.getAsJsonObject("area").get("formalName")).get("value") + " " +
                                getField(houseAddressJson.getAsJsonObject("settlement").get("shortName")).get("value") + " " +
                                getField(houseAddressJson.getAsJsonObject("settlement").get("formalName")).get("value"));
            }
            if (!houseAddressJson.get("street").isJsonNull()) {
                house.put("Street",
                        getField(houseAddressJson.getAsJsonObject("street").get("shortName")).get("value") + " " +
                                getField(houseAddressJson.getAsJsonObject("street").get("formalName")).get("value"));
                house.put("HouseNumber", getField(houseAddressJson.getAsJsonObject("house").get("houseNumber")).get("value"));
                house.put("AddressDesc", getField(houseAddressJson.get("formattedAddress")).get("value"));
            } else {
                house.put("Street", null);
            }

            house.put("houseType", getField(houseJson.getAsJsonObject("houseType").get("houseTypeName")).get("value"));
            house.put("projectType", getField(houseJson.get("planSeries")).get("value"));
            house.put("exploitationStartYear", getField(houseJson.get("operationYear")).get("value"));
            house.put("builtYear", getField(houseJson.get("buildingYear")).get("value"));
            house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), null, fieldMap));
//        house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), "3.1", params));
//        house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), "3.2", params));
//        house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), "4", params));
//        house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), "5", params));
//        house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), "6", params));
//        house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), "7", params));
//        house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), "8", params));
//        house.putAll(getFieldFromPassport(houseJson.get("guid").getAsString(), "9", params));

            return house;
        } catch (Exception e) {
            writeError(e.getMessage());
        }
        return new Document();
    }

    private Integer getCountOfHouses(String regionCode, String cityCode, String areaCode, String settlementCode) throws IOException, InterruptedException {
        boolean flag = true;
        while (flag) {
            String response = getHousesInPage(regionCode, cityCode, null, areaCode, settlementCode, null, 1, 10);
            if (response != null) {
                try {
                    JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
                    return responseJson.get("total").isJsonNull() ? 0 : responseJson.get("total").getAsInt();
                } catch (RuntimeException e) {
                    writeError(e.getMessage());
                }
            }
            Thread.sleep(20000);

        }
        return -1;
    }

    private Document getFieldFromPassport(String houseGuid, String passportParameterCode, Map<String, String> parameterNames) throws InterruptedException, IOException {

        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Host,Content-Length,Connection");
        String url = "https://dom.gosuslugi.ru/homemanagement/api/rest/services/passports/search";
        String json;
        if (passportParameterCode == null) {
            json = "{\"houseGuid\":\"" + houseGuid + "\",\"passportParameterCode\":" + null + ",\"page\":1,\"itemsPerPage\":500}";

        } else {
            json = "{\"houseGuid\":\"" + houseGuid + "\",\"passportParameterCode\":\"" + passportParameterCode + "\",\"page\":1,\"itemsPerPage\":500}";

        }
        String response = POST(url, json);
        while (response == null) {
            Thread.sleep(20000);
            response = POST(url, json);
        }

        try {
            JsonArray parameters = JsonParser.parseString(response).getAsJsonObject().getAsJsonArray("parameters");
            Document values = new Document();
            Document result = new Document();
            parameters.forEach(item -> values.put(
                    getField(item.getAsJsonObject().get("name")).get("value").toString(),
                    getField(item.getAsJsonObject().get("value")).get("value")));
            for (String parameterName : parameterNames.keySet()) {
                if (values.containsKey(parameterName)) {
                    result.put(parameterNames.get(parameterName), values.get(parameterName));
                }
            }
            return result;
        } catch (RuntimeException e) {
            writeError(e.getMessage());
        }
        return new Document();
    }

    @Override
    protected int mainHarvest() throws IOException, InterruptedException {
        String regionCode = getParams().getString("regionGuid");
        getHousesCollection().drop();
        getHousesCollection().createIndex(Indexes.ascending("title"));
        getHousesCollection().createIndex(Indexes.ascending("RegionID"));
        getHousesCollection().createIndex(Indexes.ascending("MunicipalitetID"));
        getHousesCollection().createIndex(Indexes.ascending("StreetID"));
        getHousesCollection().createIndex(Indexes.ascending("BlockID"));
        getHousesCollection().createIndex(Indexes.compoundIndex(
                Indexes.text("Region"),
                Indexes.text("Municipalitet"),
                Indexes.text("Street"),
                Indexes.text("HouseNumber")
        ));
        //Получаем список кодов городов в регионе
        String cityURL = "https://dom.gosuslugi.ru/nsi/api/rest/services/nsi/fias/v4/cities?actual=true";
        ArrayList<String> cityCodes = requestObjects(cityURL, 100, regionCode, null, null, null, null);
        String settlementURL = "https://dom.gosuslugi.ru/nsi/api/rest/services/nsi/fias/v4/settlements?actual=true";
        String streetURL = "https://dom.gosuslugi.ru/nsi/api/rest/services/nsi/fias/v4/streets?actual=true";
        for (String cityCode : cityCodes) {
            Integer countOfHouses = getCountOfHouses(regionCode, cityCode, null, null);
            if (countOfHouses > 0 && countOfHouses <= 2000) {
                ArrayList<String> ids = new ArrayList<>();
                JsonArray housesInStreet = getHousesInStreet(regionCode, cityCode, null, null, null, null, 100);
                for (int m = 0; m < housesInStreet.size(); m++) {
                    String id = getField(housesInStreet.get(m).getAsJsonObject().getAsJsonObject("address").getAsJsonObject("house").get("houseGuid")).get("value").toString();
                    if (!ids.contains(id)) {
                        try {
                            getHousesCollection().insertOne(getInfoAboutHouse(housesInStreet.get(m).getAsJsonObject()));
                            ids.add(id);
                            incCount(1);
                        } catch (IllegalStateException e) {
                            writeError(e.getMessage());
                        }
                    }
                }
            } else {
                //Получаем список кодов элементов планировочной структуры
                String elementURL = "https://dom.gosuslugi.ru/nsi/api/rest/services/nsi/fias/v4/planning/structure/elements?actual=true";
                ArrayList<String> elementCodes = requestObjects(elementURL, 100, regionCode, null, cityCode, null, null);
                for (String elementCode : elementCodes) {
                    ArrayList<String> ids = new ArrayList<>();
                    JsonArray housesInStreet = getHousesInStreet(regionCode, cityCode, null, null, null, elementCode, 100);
                    for (int m = 0; m < housesInStreet.size(); m++) {
                        String id = getField(housesInStreet.get(m).getAsJsonObject().getAsJsonObject("address").getAsJsonObject("house").get("houseGuid")).get("value").toString();
                        try {
                            getHousesCollection().insertOne(getInfoAboutHouse(housesInStreet.get(m).getAsJsonObject()));
                            ids.add(id);
                            incCount(1);
                        } catch (IllegalStateException e) {
                            writeError(e.getMessage());
                        }
                    }
                }
                //Получаем список кодов населенных пунктов, относящихся к  городу
                ArrayList<String> settlementCodes = requestObjects(settlementURL, 100, regionCode, null, cityCode, null, null);
                for (String settlementCode : settlementCodes) {
                    countOfHouses = getCountOfHouses(regionCode, cityCode, null, settlementCode);
                    if (countOfHouses > 0 && countOfHouses <= 2000) {
                        ArrayList<String> ids = new ArrayList<>();
                        JsonArray housesInStreet = getHousesInStreet(regionCode, cityCode, null, null, null, null, 100);
                        for (int m = 0; m < housesInStreet.size(); m++) {
                            String id = getField(housesInStreet.get(m).getAsJsonObject().getAsJsonObject("address").getAsJsonObject("house").get("houseGuid")).get("value").toString();
                            try {
                                getHousesCollection().insertOne(getInfoAboutHouse(housesInStreet.get(m).getAsJsonObject()));
                                ids.add(id);
                                incCount(1);
                            } catch (IllegalStateException e) {
                                writeError(e.getMessage());
                            }
                        }
                    } else {
                        ArrayList<String> ids = new ArrayList<>();
                        //получаем список кодов улиц населенного пункта, относящегося к городу
                        ArrayList<String> streetCodes = requestObjects(streetURL, 100, regionCode, null, cityCode, settlementCode, null);
                        for (String streetCode : streetCodes) {
                            JsonArray housesInStreet = getHousesInStreet(regionCode, cityCode, null, settlementCode, streetCode, null, 100);
                            for (int m = 0; m < housesInStreet.size(); m++) {
                                String id = getField(housesInStreet.get(m).getAsJsonObject().getAsJsonObject("address").getAsJsonObject("house").get("houseGuid")).get("value").toString();
                                try {
                                    getHousesCollection().insertOne(getInfoAboutHouse(housesInStreet.get(m).getAsJsonObject()));
                                    ids.add(id);
                                    incCount(1);
                                } catch (IllegalStateException e) {
                                    writeError(e.getMessage());
                                }
                            }
                        }
                    }
                }


                ArrayList<String> ids = new ArrayList<>();
                //получаем список кодов улиц города
                ArrayList<String> streetCodes = requestObjects(streetURL, 100, regionCode, null, cityCode, null, null);
                for (String streetCode : streetCodes) {
                    JsonArray housesInStreet = getHousesInStreet(regionCode, cityCode, null, null, streetCode, null, 100);
                    for (int m = 0; m < housesInStreet.size(); m++) {
                        String id = getField(housesInStreet.get(m).getAsJsonObject().getAsJsonObject("address").getAsJsonObject("house").get("houseGuid")).get("value").toString();
                        try {
                            getHousesCollection().insertOne(getInfoAboutHouse(housesInStreet.get(m).getAsJsonObject()));
                            ids.add(id);
                            incCount(1);
                        } catch (IllegalStateException e) {
                            writeError(e.getMessage());
                        }
                    }
                }

            }
        }

        //получаем список кодов районов региона
        String areaURL = "https://dom.gosuslugi.ru/nsi/api/rest/services/nsi/fias/v4/areas?actual=true";
        ArrayList<String> areaCodes = requestObjects(areaURL, 100, regionCode, null, null, null, null); //нет регионов с количество  городов большим ста
        for (String areaCode : areaCodes) {
            Integer countOfHouses = getCountOfHouses(regionCode, null, areaCode, null);
            if (countOfHouses > 0 && countOfHouses <= 2000) {
                ArrayList<String> ids = new ArrayList<>();
                JsonArray housesInStreet = getHousesInStreet(regionCode, null, areaCode, null, null, null, 100);
                for (int m = 0; m < housesInStreet.size(); m++) {
                    String id = getField(housesInStreet.get(m).getAsJsonObject().getAsJsonObject("address").getAsJsonObject("house").get("houseGuid")).get("value").toString();
                    try {
                        getHousesCollection().insertOne(getInfoAboutHouse(housesInStreet.get(m).getAsJsonObject()));
                        ids.add(id);
                        incCount(1);
                    } catch (IllegalStateException e) {
                        writeError(e.getMessage());
                    }
                }
            } else {
                //получаем список кодов населенных пунктов, относящихся к району региона
                ArrayList<String> settlementCodes = requestObjects(settlementURL, 100, regionCode, areaCode, null, null, null);
                for (String settlementCode : settlementCodes) {
                    countOfHouses = getCountOfHouses(regionCode, null, areaCode, settlementCode);
                    ArrayList<String> ids = new ArrayList<>();
                    if (countOfHouses > 0 && countOfHouses <= 2000) {
                        JsonArray housesInStreet = getHousesInStreet(regionCode, null, areaCode, settlementCode, null, null, 100);
                        for (int m = 0; m < housesInStreet.size(); m++) {
                            String id = getField(housesInStreet.get(m).getAsJsonObject().getAsJsonObject("address").getAsJsonObject("house").get("houseGuid")).get("value").toString();
                            try {
                                getHousesCollection().insertOne(getInfoAboutHouse(housesInStreet.get(m).getAsJsonObject()));
                                ids.add(id);
                                incCount(1);
                            } catch (IllegalStateException e) {
                                writeError(e.getMessage());
                            }
                        }
                    } else {
                        //получаем список кодов улиц населенного пункта, относящегося к району региона
                        ArrayList<String> streetCodes = requestObjects(streetURL, 100, regionCode, areaCode, null, settlementCode, null);
                        for (String streetCode : streetCodes) {
                            JsonArray housesInStreet = getHousesInStreet(regionCode, null, areaCode, settlementCode, streetCode, null, 100);
                            for (int m = 0; m < housesInStreet.size(); m++) {
                                String id = getField(housesInStreet.get(m).getAsJsonObject().getAsJsonObject("address").getAsJsonObject("house").get("houseGuid")).get("value").toString();
                                try {
                                    getHousesCollection().insertOne(getInfoAboutHouse(housesInStreet.get(m).getAsJsonObject()));
                                    ids.add(id);
                                    incCount(1);
                                } catch (IllegalStateException e) {
                                    writeError(e.getMessage());
                                }
                            }
                        }
                    }
                }
            }
        }
        return (int) getHousesCollection().countDocuments();
    }


}

