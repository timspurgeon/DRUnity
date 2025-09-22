
using System.Collections.Generic;

namespace Server.Store {
  public interface IStore {
    int CheckUser(string name, string pass); // returns accountId if ok, else -1
    List<CharacterRow> ListCharacters(int accountId);
    bool CreateCharacter(int accountId, string name, int classId, out string? fail);
  }

  public class CharacterRow {
    public int Id; public string Name = ""; public int ClassId; public int Level;
  }
}
