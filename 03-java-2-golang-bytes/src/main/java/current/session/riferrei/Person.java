package current.session.riferrei;

import java.io.Serializable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor
public class Person implements Serializable {

    private String userName;
    private int favoriteNumber;
    private String[] interests;

    public Person(String userName,
        int favoriteNumber, String[] interests) {
        setUserName(userName);
        setFavoriteNumber(favoriteNumber);
        setInterests(interests);
    }

}
