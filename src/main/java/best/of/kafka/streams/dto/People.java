package best.of.kafka.streams.dto;

public class People {

    private boolean isCostumed;

    public boolean isCostumed() {
        return isCostumed;
    }

    public void setCostumed(boolean costumed) {
        isCostumed = costumed;
    }

    public boolean isWearingHat() {
        return isWearingHat;
    }

    public void setWearingHat(boolean wearingHat) {
        isWearingHat = wearingHat;
    }

    private boolean isWearingHat;
}
