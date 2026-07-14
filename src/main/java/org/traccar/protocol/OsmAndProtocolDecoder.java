/*
 * Copyright 2013 - 2025 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.protocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import jakarta.json.Json;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import org.traccar.BaseHttpProtocolDecoder;
import org.traccar.config.Keys;
import org.traccar.helper.UnitsConverter;
import org.traccar.session.DeviceSession;
import org.traccar.Protocol;
import org.traccar.helper.DateUtil;
import org.traccar.model.CellTower;
import org.traccar.model.Command;
import org.traccar.model.Network;
import org.traccar.model.Position;
import org.traccar.model.WifiAccessPoint;

import java.io.StringReader;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class OsmAndProtocolDecoder extends BaseHttpProtocolDecoder {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    private static final DateTimeFormatter DEVICE_DATE_FORMAT = DateTimeFormatter
            .ofPattern("ddMMyyHHmmss").withZone(ZoneId.of("UTC"));

    private double minAccuracy;

    public OsmAndProtocolDecoder(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected void init() {
        minAccuracy = getConfig().getDouble(Keys.OSMAND_MIN_ACCURACY);
    }

    @Override
    protected Object decode(Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        FullHttpRequest request = (FullHttpRequest) msg;
        String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType != null && contentType.startsWith(HttpHeaderValues.APPLICATION_JSON.toString())) {
            return decodeJson(channel, remoteAddress, request);
        } else {
            return decodeQuery(channel, remoteAddress, request);
        }
    }

    private static String translateLoggingCode(int code) {
        return switch (code) {
            case 0x01 -> "Réinitialisation quotidienne";
            case 0x08 -> "Redémarrage après échec d'accusé de réception";
            case 0x09 -> "Redémarrage après adresse IP invalide";
            case 0x10 -> "Cyclique";
            case 0x11 -> "Keep alive";
            case 0x12 -> "Manuel (LOCSTK)";
            case 0x13 -> "Distant (SENDPOSI)";
            case 0x51 -> "Capture UART";
            case 0x58 -> "Précharge en cours";
            case 0x59 -> "Charge rapide en cours";
            case 0x5A -> "Charge terminée";
            case 0x5B -> "Charge suspendue";
            case 0x5C -> "Défaut batterie";
            case 0x5D -> "Batterie faible";
            case 0x5E -> "Alimentation externe coupée";
            case 0x5F -> "Alimentation externe branchée";
            case 0x60 -> "iButton One-Wire retiré";
            case 0x61 -> "iButton One-Wire détecté";
            case 0x80 -> "Mouvement arrêté";
            case 0x84 -> "Mouvement démarré";
            case 0x8A -> "Choc détecté";
            case 0x90 -> "Changement de cap";
            case 0x91 -> "Dépassement du seuil de vitesse basse";
            case 0x92 -> "Distance minimale atteinte";
            case 0xE0 -> "Éco-conduite : évaluation impossible";
            case 0xE1 -> "Éco-conduite : évaluation possible";
            case 0xE2 -> "Éco-conduite : freinage excessif";
            case 0xE3 -> "Éco-conduite : accélération excessive";
            case 0xE4 -> "Éco-conduite : virage excessif";
            case 0xFF -> "Inconnu";
            default -> {
                if (code >= 0x21 && code <= 0x28) {
                    yield "Entrée GPIO " + (code - 0x21 + 1);
                } else if (code >= 0x2A && code <= 0x2D) {
                    yield "Entrée analogique " + (code - 0x2A + 1);
                } else if (code >= 0xA0 && code <= 0xBD) {
                    yield "Sortie de la zone géographique " + (code - 0xA0);
                } else if (code >= 0xC0 && code <= 0xDD) {
                    yield "Entrée dans la zone géographique " + (code - 0xC0);
                } else {
                    yield null;
                }
            }
        };
    }

    private static String translateLoggingCodeAlarm(int code) {
        return switch (code) {
            case 0x5C -> Position.ALARM_FAULT;
            case 0x5D -> Position.ALARM_LOW_BATTERY;
            case 0x5E -> Position.ALARM_POWER_CUT;
            case 0x5F -> Position.ALARM_POWER_RESTORED;
            default -> null;
        };
    }

    private Object decodeQuery(
            Channel channel, SocketAddress remoteAddress, FullHttpRequest request) throws Exception {

        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> params = decoder.parameters();
        if (params.isEmpty()) {
            decoder = new QueryStringDecoder(request.content().toString(StandardCharsets.US_ASCII), false);
            params = decoder.parameters();
        }

        Position position = new Position(getProtocolName());
        position.setValid(true);

        Network network = new Network();
        Double latitude = null;
        Double longitude = null;

        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            for (String value : entry.getValue()) {
                if (value.isEmpty()) {
                    continue;
                }
                switch (entry.getKey()) {
                    case "id", "i", "deviceid" -> {
                        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, value);
                        if (deviceSession == null) {
                            sendResponse(channel, HttpResponseStatus.BAD_REQUEST);
                            return null;
                        }
                        position.setDeviceId(deviceSession.getDeviceId());
                    }
                    case "notificationToken" -> {
                        if (position.getDeviceId() > 0) {
                            getCommandsManager().updateNotificationToken(position.getDeviceId(), value);
                        }
                    }
                    case "v" -> position.setValid(value.equalsIgnoreCase("A"));
                    case "valid" -> position.setValid(Boolean.parseBoolean(value) || "1".equals(value));
                    case "timestamp" -> {
                        Date deviceTime = null;
                        if (value.length() == 12) {
                            try {
                                deviceTime = DateUtil.parse(DEVICE_DATE_FORMAT, value);
                            } catch (DateTimeParseException error) {
                                deviceTime = null;
                            }
                        }
                        if (deviceTime != null) {
                            position.setTime(deviceTime);
                        } else {
                            try {
                                long timestamp = Long.parseLong(value);
                                if (timestamp < Integer.MAX_VALUE) {
                                    timestamp *= 1000;
                                }
                                position.setTime(new Date(timestamp));
                            } catch (NumberFormatException error) {
                                if (value.contains("T")) {
                                    position.setTime(DateUtil.parseDate(value));
                                } else {
                                    position.setTime(DateUtil.parse(DATE_FORMAT, value));
                                }
                            }
                        }
                    }
                    case "d" -> position.setTime(DateUtil.parse(DEVICE_DATE_FORMAT, value));
                    case "la", "lat" -> latitude = Double.parseDouble(value);
                    case "lo", "lon" -> longitude = Double.parseDouble(value);
                    case "location" -> {
                        String[] location = value.split(",");
                        latitude = Double.parseDouble(location[0]);
                        longitude = Double.parseDouble(location[1]);
                    }
                    case "cell" -> {
                        String[] cell = value.split(",");
                        if (cell.length > 4) {
                            network.addCellTower(CellTower.from(
                                    Integer.parseInt(cell[0]), Integer.parseInt(cell[1]),
                                    Integer.parseInt(cell[2]), Integer.parseInt(cell[3]), Integer.parseInt(cell[4])));
                        } else {
                            network.addCellTower(CellTower.from(
                                    Integer.parseInt(cell[0]), Integer.parseInt(cell[1]),
                                    Integer.parseInt(cell[2]), Integer.parseInt(cell[3])));
                        }
                    }
                    case "wifi" -> {
                        String[] wifi = value.split(",");
                        network.addWifiAccessPoint(WifiAccessPoint.from(
                                wifi[0].replace('-', ':'), Integer.parseInt(wifi[1])));
                    }
                    case "speed", "s" -> position.setSpeed(convertSpeed(Double.parseDouble(value), "kmh"));
                    case "bearing", "heading" -> position.setCourse(Double.parseDouble(value));
                    case "altitude", "al" -> position.setAltitude(Double.parseDouble(value));
                    case "accuracy", "p", "prec" -> position.setAccuracy(Double.parseDouble(value));
                    case "hdop", "h" -> position.set(Position.KEY_HDOP, Double.parseDouble(value));
                    case "batt" -> position.set(Position.KEY_BATTERY_LEVEL, Double.parseDouble(value));
                    case "driverUniqueId" -> position.set(Position.KEY_DRIVER_UNIQUE_ID, value);
                    case "charge" -> position.set(Position.KEY_CHARGE, Boolean.parseBoolean(value));
                    case "in", "input" -> position.set(Position.KEY_INPUT, value);
                    case "sa", "sat" -> position.set(Position.KEY_SATELLITES, Integer.parseInt(value));
                    case "LC" -> {
                        position.set("lc", value);
                        try {
                            int code = Integer.parseInt(value, 16);
                            String description = translateLoggingCode(code);
                            if (description != null) {
                                position.set("lcDescription", description);
                            }
                            position.addAlarm(translateLoggingCodeAlarm(code));
                        } catch (NumberFormatException error) {
                            // ignore unparsable logging code
                        }
                    }
                    case "ainput" -> {
                        if (value.length() == 16) {
                            position.set("antennaVoltage", Integer.parseInt(value.substring(0, 4)));
                            position.set("analogInput1", Integer.parseInt(value.substring(4, 8)));
                            position.set("analogInput2", Integer.parseInt(value.substring(8, 12)));
                            position.set(Position.KEY_BATTERY, Integer.parseInt(value.substring(12, 16)) * 0.001);
                        } else {
                            position.set(entry.getKey(), value);
                        }
                    }
                    case "odometer" -> position.set(Position.KEY_ODOMETER, Double.parseDouble(value) * 1000);
                    default -> {
                        try {
                            position.set(entry.getKey(), Double.parseDouble(value));
                        } catch (NumberFormatException e) {
                            switch (value) {
                                case "true" -> position.set(entry.getKey(), true);
                                case "false" -> position.set(entry.getKey(), false);
                                default -> position.set(entry.getKey(), value);
                            }
                        }
                    }
                }
            }
        }

        if (position.getFixTime() == null) {
            position.setTime(new Date());
        }

        if (network.getCellTowers() != null || network.getWifiAccessPoints() != null) {
            position.setNetwork(network);
        }

        if (latitude != null && longitude != null) {
            position.setLatitude(latitude);
            position.setLongitude(longitude);
        } else {
            getLastLocation(position, position.getDeviceTime());
        }

        if (position.getDeviceId() != 0) {
            String response = null;
            for (Command command : getCommandsManager().readQueuedCommands(position.getDeviceId(), 1)) {
                response = command.getString(Command.KEY_DATA);
            }
            if (response != null) {
                sendResponse(channel, HttpResponseStatus.OK, Unpooled.copiedBuffer(response, StandardCharsets.UTF_8));
            } else {
                sendResponse(channel, HttpResponseStatus.OK);
            }
            return position;
        } else {
            sendResponse(channel, HttpResponseStatus.BAD_REQUEST);
            return null;
        }
    }

    private Object decodeJson(
            Channel channel, SocketAddress remoteAddress, FullHttpRequest request) throws Exception {

        String content = request.content().toString(StandardCharsets.UTF_8);
        JsonObject root = Json.createReader(new StringReader(content)).readObject();

        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, root.getString("device_id"));
        if (deviceSession == null) {
            sendResponse(channel, HttpResponseStatus.NOT_FOUND);
            return null;
        }

        Position position = new Position(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());

        JsonObject location = root.getJsonObject("location");

        position.setTime(DateUtil.parseDate(location.getString("timestamp")));

        if (location.containsKey("coords")) {
            JsonObject coordinates = location.getJsonObject("coords");
            position.setValid(true);
            position.setLatitude(coordinates.getJsonNumber("latitude").doubleValue());
            position.setLongitude(coordinates.getJsonNumber("longitude").doubleValue());
            double speed = coordinates.getJsonNumber("speed").doubleValue();
            if (speed >= 0) {
                position.setSpeed(UnitsConverter.knotsFromMps(speed));
            }
            double heading = coordinates.getJsonNumber("heading").doubleValue();
            if (heading >= 0) {
                position.setCourse(heading);
            }
            double accuracy = coordinates.getJsonNumber("accuracy").doubleValue();
            if (accuracy >= minAccuracy) {
                position.setAccuracy(accuracy);
            }
            position.setAltitude(coordinates.getJsonNumber("altitude").doubleValue());
        } else {
            getLastLocation(position, null);
        }

        if (location.containsKey("event")) {
            position.set(Position.KEY_EVENT, location.getString("event"));
        }
        if (location.containsKey("is_moving")) {
            position.set(Position.KEY_MOTION, location.getBoolean("is_moving"));
        }
        if (location.containsKey("odometer")) {
            position.set(Position.KEY_ODOMETER, location.getInt("odometer"));
        }
        if (location.containsKey("mock")) {
            position.set("mock", location.getBoolean("mock"));
        }
        if (location.containsKey("activity")) {
            position.set("activity", location.getJsonObject("activity").getString("type"));
        }
        if (location.containsKey("battery")) {
            JsonObject battery = location.getJsonObject("battery");
            double level = battery.getJsonNumber("level").doubleValue();
            if (level >= 0) {
                position.set(Position.KEY_BATTERY_LEVEL, (int) (level * 100));
            }
            if (battery.getBoolean("is_charging")) {
                position.set(Position.KEY_CHARGE, true);
            }
        }
        if (location.containsKey("alarm")) {
            position.set(Position.KEY_ALARM, location.getString("alarm"));
        }
        if (location.containsKey("extras")) {
            JsonObject extras = location.getJsonObject("extras");
            for (Map.Entry<String, JsonValue> extraEntry : extras.entrySet()) {
                if (extraEntry.getKey().equals("alarm") && location.containsKey("alarm")) {
                    continue;
                }
                switch (extraEntry.getValue().getValueType()) {
                    case NUMBER -> {
                        JsonNumber jsonNumber = (JsonNumber) extraEntry.getValue();
                        if (jsonNumber.isIntegral()) {
                            position.set(extraEntry.getKey(), jsonNumber.longValue());
                        } else {
                            position.set(extraEntry.getKey(), jsonNumber.doubleValue());
                        }
                    }
                    case TRUE -> position.set(extraEntry.getKey(), true);
                    case FALSE -> position.set(extraEntry.getKey(), false);
                    case STRING -> position.set(extraEntry.getKey(), ((JsonString) extraEntry.getValue()).getString());
                    default -> {}
                }
            }
        }

        sendResponse(channel, HttpResponseStatus.OK);
        return position;
    }

    @Override
    protected void sendQueuedCommands(Channel channel, SocketAddress remoteAddress, long deviceId) {}

}
