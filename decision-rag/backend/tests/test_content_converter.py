"""
Tests for content conversion service.
"""

from pathlib import Path
from unittest.mock import Mock, patch

from app.services.content_converter import (
    HTMLSanitizer,
    MarkdownConverter,
    convert_attachment_content,
    convert_decision_content,
)


class TestHTMLSanitizer:
    """Tests for HTMLSanitizer."""

    def test_sanitize_valid_html(self):
        """Test sanitizing valid HTML."""
        sanitizer = HTMLSanitizer()
        html = "<html><body><p>Test content</p></body></html>"
        result = sanitizer.sanitize(html)
        assert result is not None
        assert "Test content" in result

    def test_sanitize_malformed_html(self):
        """Test sanitizing malformed HTML."""
        sanitizer = HTMLSanitizer()
        html = "<html><body><p>Unclosed paragraph"
        result = sanitizer.sanitize(html)
        assert result is not None

    def test_sanitize_empty_content(self):
        """Test sanitizing empty content."""
        sanitizer = HTMLSanitizer()
        result = sanitizer.sanitize("")
        assert result == ""


class TestMarkdownConverter:
    """Tests for MarkdownConverter."""

    def test_convert_simple_html(self):
        """Test converting simple HTML to Markdown."""
        converter = MarkdownConverter()
        html = "<html><body><h1>Title</h1><p>Paragraph</p></body></html>"
        result = converter.convert(html)
        assert result is not None
        assert len(result) > 0

    def test_convert_empty_content(self):
        """Test converting empty content."""
        converter = MarkdownConverter()
        result = converter.convert("")
        assert result == ""

    def test_convert_decision_content(self):
        """Test the main entry point function."""
        html = "<html><body><div class='paatos'><h1>Decision</h1><p>Content</p></div></body></html>"
        result = convert_decision_content(html)
        assert result is not None

    def test_realistic_html_conversion(self):
        """Test converting more complex HTML content."""
        converter = MarkdownConverter()
        html = """
        <html lang="fi"><head><meta content="text/html; charset=utf-8" http-equiv="Content-Type"><meta content="{67537D98-A192-C905-9D76-99A35AC00002}" name="DhId"><meta content="2025-12-16T14:50:56.767+02:00"
name="ThisHTMLGenerated"><title>Takautumisvaatimus, liukastuminen, LähiTapiola Keskinäinen Vakuutusyhtiö</title></head><body><div class="paatos"><div class="Otsikonviite"></div><div class="Otsikonviite2"></div><div class="Asiapykala">46
§</div><h1 class="AsiaOtsikko">Takautumisvaatimus, liukastuminen, LähiTapiola Keskinäinen Vakuutusyhtiö</h1><div class="DnroTmuoto">HEL 2025-005744 T 03 01 00</div><div class="Viite"></div><div class="Viite"></div><div
class="SisaltoSektio"><h3 class="SisaltoOtsikko">Päätös</h3><div><p>Lakipalvelut-yksikön päällikkö päätti hylätä hakijan takautumisvaatimuksen.</p></div></div><div class="SisaltoSektio"><h3 class="SisaltoOtsikko">Päätöksen
perustelut</h3><div><h4>Hakijan vaatimus</h4><p>Hakija on 1.4.2025 esittänyt kaupungille määrältään 9 555,77 euron takautumisvaatimuksen. Vaatimuksen mukaan vakuutusyhtiöllä on takautumisoikeus kaupunkia kohtaan maksamassaan
liukastumisvahingossa numero 3502411666 työtapaturma- ja ammattitautilain 270 §:n perusteella. Vaatimuksen mukaan vahingonkärsijä liukastui 21.2.2024 klo 13.30 osoitteessa Hillerikuja 4, Hertsikan ala-asteen toimipisteellä talon kulmalla.
Vahingonkärsijän olosuhdeselvityksen mukaan vahinkopaikka oli hiekoitettu, mutta vahinkopaikalla oli noin 50 cm x 50 cm alue, jossa oli jäätä eikä pienen lumikerroksen alla ollut hiekkaa. Lunta ei ollut niin paljon, että sitä olisi tarvinnut
aurata. Hakija on toimittanut vaatimuksen yhteydessä kartan, johon vahinkopaikka on merkitty.</p><h4>Sovellettavat normit ja ohjeet</h4><p>Kiinteistö, jonka alueella vahinko on tapahtunut, on kaupungin omistuksessa, joten kaupungin
vahingonkorvausvelvollisuuden syntymistä arvioidaan sekä vahingonkorvauslain vastuusäännösten että maankäyttö- ja rakennuslain kunnossapitoa koskevien säännösten kautta. Vahingon tapahtuma-aikaan voimassa ollut maankäyttö- ja rakennuslaki
velvoitti pitämään rakennuksen ympäristöineen sellaisessa kunnossa, että se jatkuvasti täyttää terveellisyyden, turvallisuuden ja käyttökelpoisuuden vaatimukset. Kaupungilla on kiinteistön omistajana vastuu alueen kunnossapitotoimien
toteuttamisesta. Kaupunki on siirtänyt sopimuksin kyseisen alueen kunnossapidon palveluyhtiölle.</p><p>Kaupungin ja kunnossapitoa suorittavan urakoitsijan välisen sopimuksen mukaan kohteessa liukkaudentorjunta sekä lumityöt tulee tehdä
ulko-ovien edustoilla sekä pääkulkuväylillä arkisin klo 9 mennessä ja pyhäisin klo 12 mennessä tai kaksi tuntia liukkauden syntymisestä. Jatkuvan tai päiväaikaisen toistuvan pyryn, räntäsateen tai jäätävän sateen aikana tulee lumen, loskan ja
jään poistamisesta huolehtia siten, että lumi- tai loskakerroksen paksuus ei ole yli 5 cm. </p><h4>Vahingonkorvausvastuun edellytykset</h4><p>Pelkkä vahinkotapahtuma ei yksinään aiheuta kaupungille korvausvastuuta.
</p><p>Vahingonkorvausvastuu perustuu lähtökohtaisesti tuottamukseen, eli kunnossapitäjän on vahingonkorvausvastuun syntymiseksi täytynyt syyllistyä johonkin laiminlyöntiin, huolimattomuuteen tai virheelliseen menettelyyn. Lisäksi
edellytetään, että toiminnan tai laiminlyönnin ja syntyneen vahingon välillä on syy-yhteys.</p><p>Alueen kunnossapitovelvollinen vapautuu vahingonkorvausvastuustaan osoittamalla, että kunnossapito on hoidettu asianmukaisesti tai että
vallinneet olosuhteet ovat tehneet kunnossapitotyöt hyödyttömiksi tai että kunnossapidosta huolehtiminen olisi vahinkohetken olosuhteissa ollut ylivoimaista.  </p><p>Jos vahingon kärsineen puolelta on myötävaikutettu vahinkoon tai jos muu
vahingon aiheuttaneeseen tekoon kuulumaton seikka on myös ollut vahingon syynä, voidaan vahingonkorvausta kohtuuden mukaan sovitella tai evätä kokonaan. </p><h4>Asiassa saadut tiedot ja selvitykset </h4><p>Vahinkopaikan
kunnossapitotoimenpiteistä on pyydetty selvitys kiinteistön kunnossapidosta vastaavalta. Saadun selvityksen mukaan vahinkopaikka on hiekoitettu kaksi päivää ennen vahinkoa 19.2.2024 klo 13 ja aurattu 14.2.2024 klo 3. Vahinkopaikalla on tehty
käsilumitöitä 18.2.2024 klo 7–10 välillä.  Kunnossapidon tietoon ei ole tullut palautteita vahinkopaikan liukkaudentorjunnan tarpeesta vahinkopäivältä. Selvitys perustuu päiväkirjamerkintöihin.</p><p>Forecan mukaan Helsingin Kumpulan
havaintoasemalla tehdyissä mittauksissa vahinkopäivänä 21.2.2024 lämpötila on vaihdellut -0,8 °C ja 0,1 °C välillä ennen vahinkoa. Vahinkopäivänä klo 13 lämpötila on ollut 0,1 °C ja sää on ollut pilvinen. Vuorokauden aikana esiintyi vähäistä
lumisadetta. Lumensyvyys on ollut Kumpulan havaintoasemalla vuorokauden alussa 37 cm ja vuorokauden lopussa 37 cm. 18.2.2024 klo 10 lumensyvyys on ollut 37 cm. 18.–20.2.2024 ei ole esiintynyt sateita. Vahinkoa edeltävänä päivänä 20.2.2024
lämpötila on vaihdellut -1,3 °C ja 0,0 °C välillä. Kaksi päivää ennen vahinkoa 19.2.2024 lämpötila on vaihdellut -6,5 °C ja -0,3 °C välillä.</p><h4>Olosuhteiden huomioonottaminen ja muut asiaan vaikuttavat tekijät</h4><p>Liukkauden torjuntaa
ei ole mahdollista toteuttaa siten, että kulkuväylät eivät koskaan olisi liukkaat (esim. KKO:1998:147). Liukkaudentorjuntatoimenpiteillä ei aina voida täysin estää jään aiheuttamaa liukkautta siinäkään tapauksessa, että kunnossapito on ollut
asianmukaisella ja kunnossapitolain edellyttämällä tasolla. Aina on jokin kohta, johon on mahdollista liukastua. (Vakuutuslautakunnan ratkaisu FINE-001397.) </p><h4>Yhteenveto ja johtopäätökset</h4><p>Asiassa saatujen selvitysten perusteella
vahinkopaikan talvikunnossapito ennen vahinkoa on tehty kaupungin normien mukaisesti. Vahinkopaikka on hiekoitettu kaksi päivää ennen vahinkoa, ja vahinkopaikalla on tehty käsilumitöitä kolme päivää ennen vahinkoa.  Vahinkopaikalla tehtyjen
edellisten toimenpiteiden jälkeen on esiintynyt vain vähäisiä lumisateita, jotka eivät ole vaikuttaneet lumen syvyyteen, eikä aurauskynnys ole ylittynyt ennen vahinkoa. Vahinkoaikaan vallinneet sääolosuhteet eivät ole aiheuttaneet
vahinkopaikalla enempiä liukkaudentorjunnan tarpeita. Myös vahingonkärsijän selvityksen mukaan alue on ollut hiekoitettu lukuun ottamatta pientä jääaluetta. Muita palautteita tai muita vahinkoja koskevia vahingonkorvausvaatimuksia ei
kaupungille vahinkopaikan liukkauden takia ole tullut. Jalankulkijan on talvisissa olosuhteissa noudatettava erityistä huolellisuutta ja varovaisuutta, koska aina on jokin kohta, johon voi liukastua.</p><p>Vallitseva oikeuskäytäntö, asiassa
saadut selvitykset ja vallinneet sääolot huomioon ottaen kaupunki katsoo, ettei se ole laiminlyönyt kunnossapitolain mukaisia tehtäviään, eikä ole asiassa korvausvelvollinen.</p><h4>Sovelletut säännökset</h4><p>Vahingonkorvauslaki (412/1974)
2 luku 1 §, 6 luku 1 §</p><p>Laki kadun ja eräiden yleisten alueiden kunnossa- ja puhtaanapidosta (669/1978) 1–4 §</p><p>Tieliikennelaki (729/2018) 3 §</p><p>Työtapaturma- ja ammattitautilaki (459/2015) 270 §
</p><h4>Toimivalta</h4><p>Kaupunkiympäristön toimialajohtajan 22.12.2023 tekemän päätöksen § 51 mukaan lakipalvelut-yksikön päällikkö päättää toimialaa koskevasta vahingonkorvauksesta silloin, kun vahingonkorvausvaatimuksen määrä on enintään
15 000 euroa. </p></div></div><h3 class="LisatiedotOtsikko">Lisätiedot</h3><p>Paula Karppinen, valmistelija, puhelin: 09 310 52908</p><div>kymp.korvausasiat@hel.fi</div><p></p><div class="SahkoinenAllekirjoitusSektio"><p
class="SahkoisestiAllekirjoitettuTeksti">Päätös on sähköisesti allekirjoitettu.</p><p></p><div class="Puheenjohtajanimi">Kaisu Tähtinen</div><div class="Puheenjohtajaotsikko">yksikön päällikkö</div><p></p></div><h3
class="MuutoksenhakuOtsikko">Muutoksenhaku</h3><h4>OHJEET OIKAISUVAATIMUKSEN TEKEMISEKSI</h4><p>Tähän päätökseen tyytymätön voi tehdä kirjallisen oikaisuvaatimuksen. Päätökseen ei saa hakea muutosta valittamalla
tuomioistuimeen.</p><h5>Oikaisuvaatimusoikeus</h5><p>Oikaisuvaatimuksen saa tehdä</p><div><ul><li>se, johon päätös on kohdistettu tai jonka oikeuteen, velvollisuuteen tai etuun päätös välittömästi vaikuttaa (asianosainen)</li><li>kunnan
jäsen.</li></ul></div><h5>Oikaisuvaatimusaika</h5><p>Oikaisuvaatimus on tehtävä 14 päivän kuluessa päätöksen tiedoksisaannista.</p><p>Oikaisuvaatimuksen on saavuttava Helsingin kaupungin kirjaamoon määräajan viimeisenä päivänä ennen kirjaamon
aukioloajan päättymistä.</p><p>Mikäli päätös on annettu tiedoksi postitse, asianosaisen katsotaan saaneen päätöksestä tiedon, jollei muuta näytetä, seitsemän päivän kuluttua kirjeen lähettämisestä. Kunnan jäsenen katsotaan saaneen päätöksestä
tiedon seitsemän päivän kuluttua siitä, kun pöytäkirja on nähtävänä yleisessä tietoverkossa.</p><p>Mikäli päätös on annettu tiedoksi sähköisenä viestinä, asianosaisen katsotaan saaneen päätöksestä tiedon, jollei muuta näytetä, kolmen päivän
kuluttua viestin lähettämisestä.</p><p>Tiedoksisaantipäivää ei lueta oikaisuvaatimusaikaan. Jos oikaisuvaatimusajan viimeinen päivä on pyhäpäivä, itsenäisyyspäivä, vapunpäivä, joulu- tai juhannusaatto tai arkilauantai, saa oikaisuvaatimuksen
tehdä ensimmäisenä arkipäivänä sen jälkeen.</p><h5>Oikaisuvaatimusviranomainen</h5><p>Viranomainen, jolle oikaisuvaatimus tehdään, on Helsingin  kaupungin kaupunkiympäristölautakunta.</p><p>Oikaisuvaatimusviranomaisen asiointiosoite on
seuraava:</p><p>Suojattu sähköposti:        https://securemail.hel.fi/ </p><p>Käytäthän aina suojattua sähköpostia, kun lähetät henkilökohtaisia tietojasi.</p><p>Muistathan asioinnin yhteydessä mainita kirjaamisnumeron (esim. HEL
2021-000123), mikäli asiasi on jo vireillä Helsingin kaupungissa.</p><div><div><table><colgroup><col width="24.25%"><col
width="75.75%"></colgroup><tbody><tr><td><div>Sähköpostiosoite:</div></td><td><div>helsinki.kirjaamo@hel.fi</div></td></tr><tr><td><div>Postiosoite:</div></td><td><div>PL 10</div></td></tr><tr><td><div> </div></td><td><div>00099 HELSINGIN
KAUPUNKI</div></td></tr><tr><td><div>Käyntiosoite:</div></td><td><div>Pohjoisesplanadi 11-13</div></td></tr><tr><td><div>Puhelinnumero:</div></td><td><div>09 310 13700</div></td></tr></tbody></table></div></div><p>Kirjaamon aukioloaika on
maanantaista perjantaihin klo 08.15–16.00.</p><h5>Oikaisuvaatimuksen muoto ja sisältö</h5><p>Oikaisuvaatimus on tehtävä kirjallisena. Myös sähköinen asiakirja täyttää vaatimuksen kirjallisesta muodosta.</p><p>Oikaisuvaatimuksessa on
ilmoitettava</p><div><ul><li>päätös, johon oikaisuvaatimus kohdistuu</li><li>miten päätöstä halutaan oikaistavaksi</li><li>millä perusteella päätöstä halutaan oikaistavaksi</li><li>oikaisuvaatimuksen tekijä</li><li>millä perusteella
oikaisuvaatimuksen tekijä on oikeutettu tekemään vaatimuksen</li><li>oikaisuvaatimuksen tekijän yhteystiedot</li></ul></div><h5>Pöytäkirja</h5><p>Päätöstä koskevia pöytäkirjan otteita ja liitteitä lähetetään pyynnöstä. Asiakirjoja voi tilata
Helsingin kaupungin kirjaamosta.</p><h3 class="OtteetOtsikko">Otteet</h3><table><thead><tr><th>Ote</th><th>Otteen liitteet</th></tr></thead><tbody><tr><td>Hakija</td><td><div>Oikaisuvaatimusohje,
kaupunkiympäristölautakunta</div></td></tr></tbody></table></div></body></html>
        """
        result = converter.convert(html)
        # Output the result to a file
        with open("test_output.md", "w", encoding="utf-8") as f:
            f.write(result)
        assert result is not None


class TestMarkdownCleaning:
    """Tests for Markdown cleaning."""

    def test_clean_excessive_newlines(self):
        """Test removal of excessive blank lines."""
        converter = MarkdownConverter()
        text = "Line 1\n\n\n\n\nLine 2"
        cleaned = converter._clean_markdown(text)
        assert "\n\n\n" not in cleaned

    def test_clean_trailing_whitespace(self):
        """Test removal of trailing whitespace."""
        converter = MarkdownConverter()
        text = "Line 1   \nLine 2  "
        cleaned = converter._clean_markdown(text)
        assert "   \n" not in cleaned


class TestAttachmentConversion:
    """Tests for attachment file conversion."""

    @patch("app.services.content_converter.DocumentConverter.convert")
    def test_convert_attachment_file_success(self, mock_convert, tmp_path):
        """Test successful attachment file conversion."""
        # Create a temporary file
        test_file = tmp_path / "test.pdf"
        test_file.write_bytes(b"PDF content")

        # Mock Docling result
        mock_document = Mock()
        mock_document.export_to_markdown.return_value = "# Test Document\n\nThis is converted content."
        mock_result = Mock()
        mock_result.document = mock_document
        mock_convert.return_value = mock_result

        converter = MarkdownConverter()
        result = converter.convert_attachment_file(test_file)

        assert result is not None
        assert "Test Document" in result
        assert len(result) > 0

    def test_convert_attachment_file_nonexistent(self):
        """Test conversion of non-existent file."""
        converter = MarkdownConverter()
        result = converter.convert_attachment_file(Path("/nonexistent/file.pdf"))
        assert result == ""

    def test_convert_attachment_file_none(self):
        """Test conversion with None path."""
        converter = MarkdownConverter()
        result = converter.convert_attachment_file(None)
        assert result == ""

    @patch("app.services.content_converter.DocumentConverter.convert")
    def test_convert_attachment_content_function(self, mock_convert, tmp_path):
        """Test the convert_attachment_content entry point function."""
        # Create a temporary file
        test_file = tmp_path / "test.docx"
        test_file.write_bytes(b"DOCX content")

        # Mock Docling result
        mock_document = Mock()
        mock_document.export_to_markdown.return_value = "Converted DOCX content"
        mock_result = Mock()
        mock_result.document = mock_document
        mock_convert.return_value = mock_result

        result = convert_attachment_content(test_file)
        assert result == "Converted DOCX content\n"

    @patch("app.services.content_converter.DocumentConverter.convert")
    def test_convert_attachment_file_error_handling(self, mock_convert, tmp_path):
        """Test error handling during attachment conversion."""
        test_file = tmp_path / "test.pdf"
        test_file.write_bytes(b"PDF content")

        # Mock conversion error
        mock_convert.side_effect = Exception("Conversion failed")

        converter = MarkdownConverter()
        result = converter.convert_attachment_file(test_file)

        # Should return empty string on error
        assert result == ""


class TestAppealSectionRemoval:
    """Tests for removal of MuutoksenhakuohjeetSektio sections."""

    def test_removes_muutoksenhakuohjeet_section(self):
        """Appeal section is absent from output after sanitization."""
        sanitizer = HTMLSanitizer()
        html = (
            "<html><body>"
            "<section class=\"MuutoksenhakuohjeetSektio\"><p>Appeal instructions</p></section>"
            "<p>Other content</p>"
            "</body></html>"
        )
        result = sanitizer.sanitize(html)
        assert "MuutoksenhakuohjeetSektio" not in result
        assert "Appeal instructions" not in result
        assert "Other content" in result

    def test_keeps_other_sections_intact(self):
        """Non-appeal sections are preserved after sanitization."""
        sanitizer = HTMLSanitizer()
        html = (
            "<html><body>"
            "<section class=\"SisaltoSektio\"><p>Decision text</p></section>"
            "<section class=\"MuutoksenhakuohjeetSektio\"><p>Appeal boilerplate</p></section>"
            "</body></html>"
        )
        result = sanitizer.sanitize(html)
        assert "SisaltoSektio" in result
        assert "Decision text" in result
        assert "Appeal boilerplate" not in result

    def test_no_appeal_section_present(self):
        """HTML without any appeal section passes through unchanged."""
        sanitizer = HTMLSanitizer()
        html = "<html><body><p>Just a paragraph</p></body></html>"
        result = sanitizer.sanitize(html)
        assert "Just a paragraph" in result

    def test_multiple_appeal_sections_removed(self):
        """All appeal section instances are removed when more than one exists."""
        sanitizer = HTMLSanitizer()
        html = (
            "<html><body>"
            "<section class=\"MuutoksenhakuohjeetSektio\"><p>First appeal block</p></section>"
            "<p>Middle content</p>"
            "<section class=\"MuutoksenhakuohjeetSektio\"><p>Second appeal block</p></section>"
            "</body></html>"
        )
        result = sanitizer.sanitize(html)
        assert "First appeal block" not in result
        assert "Second appeal block" not in result
        assert "Middle content" in result
