Feature: As a developer, I want to be able to setup and verify database contents in
  godog features.

  Scenario: It should be possible to insert rows into a table and verify row counts.

    When I have following rows in table "foo":
      | id | text |
      | 1  | bar  |
    Then I should have 1 row in table "foo"

  Scenario: It should be possible to empty a table.

    Given I have following rows in table "foo":
      | id | text |
      | 1  | bar  |
    When the table "foo" is empty
    Then the table "foo" should be empty

  Scenario: There should be the expected rows in the table.

    When I have following rows in table "foo":
      | id | text |
      | 1  | bar  |
      | 2  | baz  |
      | 3  | foo  |
    Then I should have 3 rows in table "foo"
    And I should have following rows in table "foo":
      | id | text |
      | 2  | baz  |
      | 1  | bar  |

  Scenario: There should be only the expected rows in the table.

    When I have following rows in table "foo":
      | id | text |
      | 1  | bar  |
      | 2  | baz  |
      | 3  | foo  |
    Then I should have only following rows in table "foo":
      | id | text |
      | 3  | foo  |
      | 2  | baz  |
      | 1  | bar  |

  Scenario: Not all columns of a table need to be specified when matching a row.

    When I have following rows in table "foo":
      | id | text |
      | 1  | bar  |
    Then I should have following rows in table "foo":
      | id |
      | 1  |
